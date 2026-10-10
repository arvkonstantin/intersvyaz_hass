"""FCM-слушатель звонков в домофон (библиотека firebase-messaging).

Интерсвязь шлёт push-уведомление о звонке в домофон как FCM data-пуш.
Обычно такой пуш получает и разбирает установленное Android-приложение —
этот модуль вместо этого регистрирует Home Assistant как ещё одно
"виртуальное" устройство напрямую в Firebase-проекте Интерсвязи
(intersvyazlk) и слушает те же самые пуши без какого-либо реального
телефона.

ВНИМАНИЕ ПРО ВЫБОР БИБЛИОТЕКИ: пробовали переключиться на pullkin (более
современная, async-native альтернатива) — технически она даёт нормальное
супервизирование задачи, но выяснилось, что её процесс регистрации у
Google (gcm_check_in) выдаёт себя за Chrome OS/браузер (chrome_build,
DeviceType.DEVICE_CHROME_OS), а не за Android-устройство — и в итоге НИ
РАЗУ не получила ни одного сообщения за много часов тестирования, даже
несмотря на полностью стабильное, правильно залогиненное соединение.
firebase-messaging, при всех её проблемах со стабильностью соединения,
РЕАЛЬНО получает пуши — поэтому вернулись на неё, перенеся сюда все уроки,
полученные при работе над pullkin (супервизирование, умная перерегистрация
на бэкенде, диагностика).

Известный баг firebase-messaging: после определённых типов разрыва
соединения (Google сбрасывает MCS-сокет) внутренняя обработка ошибок
библиотеки иногда зависает и не восстанавливается сама, см.
https://github.com/bvis/fermax-blue-hass/issues/12 — тот же баг в очень
похожем проекте. У библиотеки нет доступа снаружи к её внутренней задаче-
слушателю (в отличие от pullkin), поэтому вместо прямого супервизирования
задачи используется ВРЕМЕННОЙ супервизор — плановое обновление раз в
WATCHDOG_TIMEOUT, независимо от того, есть трафик или нет (в реальном
использовании нет гарантированного "фонового пульса", на который можно
было бы полагаться как на индикатор жизни).

Регистрация происходит в двух независимых списках на бэкенде Интерсвязи
(подтверждено практикой):
  1. api.is74.ru/mobile/pushtoken/add-with-device-id — общий список
  2. td-crm.is74.ru/user-device — отдельный бэкенд именно для звонков
     (нужна отдельная авторизация через auth-lk, buyerId всегда 1)

Классификация пушей: у "обычных" уведомлений в payload есть непустое поле
"data" с title/text/type. У предполагаемого звонка data пустая/отсутствует
— то же самое верно и для фонового keep-alive FCM, надёжного способа
отличить их по содержимому не найдено (см. историю чата).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid as uuid_lib
from datetime import datetime, timezone
from typing import Any, Callable

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from .api import IntersvyazApiClient, IntersvyazApiError
from .const import DOMAIN, EVENT_INCOMING_CALL

_LOGGER = logging.getLogger(__name__)

# Firebase-конфиг проекта intersvyazlk — найден разбором resources.arsc
# из APK com.intersvyaz.lk (версия 2.19.0/2.20.0).
FCM_PROJECT_ID = "intersvyazlk"
FCM_APP_ID = "1:361180765175:android:9c0fafffa6c60062"
FCM_API_KEY = "AIzaSyCWGN-JHGm50OpAo3-2gR7l1kCQIEs7YO4"
FCM_SENDER_ID = "361180765175"

TD_APP_ID = "com.intersvyaz.lk"

STORAGE_VERSION = 1
STORAGE_KEY_PREFIX = f"{DOMAIN}_fcm_credentials"

# Плановое обновление соединения — не индикатор смерти (такого надёжного
# индикатора у нас для firebase-messaging нет), а просто бережная страховка
# на случай, если соединение тихо умерло, а библиотека сама не заметила.
WATCHDOG_CHECK_INTERVAL = 120  # сек (2 минуты) — как часто проверяем
WATCHDOG_TIMEOUT = 15 * 60  # сек (15 минут)
BACKEND_REREGISTER_MAX_AGE = 6 * 60 * 60  # сек (6 часов) — подстраховка:
# принудительно подтверждаем регистрацию на бэкенде, даже если токен не
# менялся — на случай серверной причины, не связанной со сменой токена.


class FcmCallListener:
    """Держит соединение с FCM, регистрирует устройство в двух списках,
    классифицирует входящие пуши и пробрасывает предполагаемые звонки."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: IntersvyazApiClient,
        phone: str,
        entry_id: str,
    ) -> None:
        self._hass = hass
        self._client = client
        self._phone = phone
        self._entry_id = entry_id
        self._store: Store = Store(hass, STORAGE_VERSION, f"{STORAGE_KEY_PREFIX}_{entry_id}")
        self._fcm_client = None  # type: ignore[assignment]
        self._unique_device_id: str | None = None
        self.fcm_token: str | None = None

        self._watchdog_task: asyncio.Task | None = None
        self._last_backend_register_at: float = 0.0
        self._last_connect_at: float = 0.0

        self.on_call_detected: Callable[[], None] | None = None
        self._save_lock = asyncio.Lock()

        suffix = entry_id.lower()
        self._status_entity_id = f"sensor.intersvyaz_fcm_status_{suffix}"
        self._last_push_entity_id = f"sensor.intersvyaz_last_push_{suffix}"

    def _set_status(self, state: str, **attrs: Any) -> None:
        self._hass.states.async_set(
            self._status_entity_id,
            state,
            {"friendly_name": "Интерсвязь — статус FCM-слушателя", **attrs},
        )

    # ------------------------------------------------------------------ #
    # Запуск: FCM-соединение + регистрация в обоих списках на бэкенде
    # ------------------------------------------------------------------ #
    async def async_start(self) -> None:
        self._set_status("подключение")

        stored = await self._store.async_load() or {}
        self._unique_device_id = stored.get("unique_device_id")
        if not self._unique_device_id:
            self._unique_device_id = uuid_lib.uuid4().hex
        await self._save_store(unique_device_id=self._unique_device_id)

        fcm_token = await self._connect_fcm()
        if not fcm_token:
            return  # _connect_fcm уже выставил статус ошибки

        await self._register_with_backend(fcm_token)

        # async_create_background_task (НЕ async_create_task) — задача
        # крутится бесконечно, и HA не должна ждать её завершения при
        # старте/остановке (иначе bootstrap виснет на старте).
        self._watchdog_task = self._hass.async_create_background_task(
            self._watchdog_loop(), name="intersvyaz_fcm_watchdog"
        )
        self._set_status("слушает", unique_device_id=self._unique_device_id)

    async def _connect_fcm(self) -> str | None:
        """Создаёт (или пересоздаёт при переподключении) FcmPushClient,
        делает checkin_or_register и start(). Возвращает fcm_token или
        None при ошибке (тогда статус уже выставлен)."""
        from firebase_messaging import FcmRegisterConfig
        from .fcm_client import IntersvyazFcmPushClient

        stored = await self._store.async_load() or {}
        fcm_creds = stored.get("fcm_credentials")

        fcm_config = FcmRegisterConfig(FCM_PROJECT_ID, FCM_APP_ID, FCM_API_KEY, FCM_SENDER_ID)

        async def _save_credentials(creds: dict) -> None:
            await self._save_store(fcm_credentials=creds)

        def _on_credentials_updated(creds: dict) -> None:
            self._hass.async_create_task(_save_credentials(creds))

        # ВАЖНО: сигнатура FcmPushClient — (callback, fcm_config, credentials,
        # credentials_updated_callback, ...) — callback обязателен сразу в
        # конструкторе, задать его позже через атрибут нельзя.
        self._fcm_client = IntersvyazFcmPushClient(
            self._on_notification,
            fcm_config,
            fcm_creds,
            _on_credentials_updated,
        )

        try:
            fcm_token = await self._fcm_client.checkin_or_register()
            await self._fcm_client.start()
        except Exception as err:  # noqa: BLE001 — переподключение не должно падать молча
            _LOGGER.exception("Не удалось подключиться к FCM")
            self._set_status("ошибка подключения к FCM", error=str(err))
            return None

        self.fcm_token = fcm_token
        self._last_connect_at = time.monotonic()
        _LOGGER.debug("FCM-токен получен (значение скрыто)")
        return fcm_token

    async def _register_with_backend(self, fcm_token: str) -> None:
        """Регистрирует (или ПЕРЕрегистрирует) fcm_token в обоих списках
        на бэкенде Интерсвязи. Вызывается при первом запуске и при
        переподключении, если токен реально изменился ИЛИ давно не
        подтверждался (BACKEND_REREGISTER_MAX_AGE) — не на каждый мелкий
        реконнект, чтобы не выглядеть подозрительно частым для их бэкенда
        (настоящее приложение тоже так не делает)."""
        self._set_status("токен получен, регистрация на бэкенде")
        self._last_backend_register_at = time.monotonic()

        stored = await self._store.async_load() or {}
        cached_td_token = stored.get("td_token")

        try:
            await self._client.register_push_token(
                fcm_token=fcm_token,
                unique_device_id=self._unique_device_id,
                authorize_phone=self._phone,
            )
        except IntersvyazApiError as err:
            _LOGGER.error("Регистрация в api.is74.ru не удалась: %s", err)

        td_token: str | None = None
        if cached_td_token and await self._client.check_td_token(cached_td_token):
            _LOGGER.info("Переиспользую живой td-токен из кеша (без новой сессии auth-lk)")
            td_token = cached_td_token
        else:
            try:
                td_auth = await self._client.auth_lk()
                td_token = td_auth["TOKEN"]
            except IntersvyazApiError as err:
                _LOGGER.error("auth-lk не удался: %s", err)
                self._set_status("ошибка auth-lk", error=str(err))

        if not td_token:
            return

        await self._save_store(td_token=td_token)
        try:
            await self._client.register_td_device(
                td_token,
                app_id=TD_APP_ID,
                push_token=fcm_token,
                platform="android",
                device_name="Home Assistant",
                device_id=self._unique_device_id,
                sending_push=True,
                notification_type="push",
            )
            _LOGGER.info("Устройство зарегистрировано в td-crm (user-device)")
        except IntersvyazApiError as err:
            _LOGGER.error("user-device не удался: %s", err)
            self._set_status("ошибка user-device", error=str(err))
            return

        # Диагностика: спрашиваем у сервера, как ОН видит нашу регистрацию.
        settings_entity_id = f"sensor.intersvyaz_device_settings_{self._entry_id.lower()}"
        try:
            settings = await self._client.get_device_settings(td_token, self._unique_device_id, TD_APP_ID)
            device_info = settings.get("device_info", {}) if isinstance(settings, dict) else {}
            notification_type = device_info.get("notificationType") or device_info.get("NOTIFICATION_TYPE")
            self._hass.states.async_set(
                settings_entity_id,
                notification_type or "неизвестно",
                {"friendly_name": "Интерсвязь — статус устройства (глазами сервера)", **{k: device_info[k] for k in ("notificationType", "sendingPush", "platform") if k in device_info}},
            )
        except IntersvyazApiError as err:
            _LOGGER.error("get_device_settings не удался: %s", err)

    async def _save_store(self, **updates: Any) -> None:
        async with self._save_lock:
            current = await self._store.async_load() or {}
            current.update(updates)
            await self._store.async_save(current)

    # ------------------------------------------------------------------ #
    # Временной супервизор (плановое обновление, не индикатор смерти)
    # ------------------------------------------------------------------ #
    async def _watchdog_loop(self) -> None:
        while True:
            await asyncio.sleep(WATCHDOG_CHECK_INTERVAL)
            age = time.monotonic() - self._last_connect_at
            if age <= WATCHDOG_TIMEOUT:
                continue

            _LOGGER.info("FCM: плановое обновление соединения (%.0f сек с последнего подключения)", age)
            self._set_status("плановое переподключение")

            if self._fcm_client is not None:
                try:
                    await self._fcm_client.stop()
                except Exception:  # noqa: BLE001
                    pass

            previous_token = self.fcm_token
            fcm_token = await self._connect_fcm()
            if fcm_token:
                if (
                    fcm_token != previous_token
                    or time.monotonic() - self._last_backend_register_at > BACKEND_REREGISTER_MAX_AGE
                ):
                    await self._register_with_backend(fcm_token)
                self._set_status("слушает (переподключились)", unique_device_id=self._unique_device_id)

    # ------------------------------------------------------------------ #
    # Разбор входящих пушей
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_data(value: Any) -> dict[str, Any] | None:
        """Защищённо достаёт dict с полями пуша из value, каким бы оно ни
        пришло — уже готовым dict, вложенным под "data", JSON-строкой, или
        чем-то совсем неожиданным (тогда просто None, без исключения)."""
        if isinstance(value, dict):
            inner = value.get("data")
            return inner if isinstance(inner, dict) else value
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except (ValueError, TypeError):
                return None
            return parsed if isinstance(parsed, dict) else None
        return None

    def _on_notification(self, obj: Any, notification: Any, data_message: Any) -> None:
        """Callback firebase-messaging на каждый входящий пуш.

        ВАЖНО: obj/notification могут прийти НЕ словарями (например, для
        чисто data-пушей без видимого уведомления notification бывает
        строкой) — весь разбор ниже защищён от этого явно, без опоры на
        try/except, чтобы push не терялся молча при неожиданной форме.
        """
        data = self._extract_data(obj) or self._extract_data(notification) or {}
        has_data = isinstance(obj, dict) and bool(obj.get("data"))
        now_iso = datetime.now(timezone.utc).isoformat()
        message_id = (obj.get("fcmMessageId") if isinstance(obj, dict) else None) or (notification if isinstance(notification, str) else None)

        _LOGGER.debug(
            "Входящий FCM-пуш: has_data=%s", has_data,
        )

        decode_error = isinstance(obj, dict) and bool(obj.get("_intersvyaz_decode_error"))
        assumed_call = isinstance(obj, dict) and not has_data and not decode_error

        self._hass.states.async_set(
            self._last_push_entity_id,
            (data.get("type") or data.get("push_type") or ("ошибка расшифровки" if decode_error else ("ЗВОНОК?" if assumed_call else "unknown"))),
            {
                "friendly_name": "Интерсвязь — последний пуш",
                "received_at": now_iso,
                "assumed_call": assumed_call,
                "has_data_field": has_data,
                "message_id": message_id,
                "data_type": data.get("type"),
                "push_type": data.get("push_type"),
                "title": data.get("title"),
                "text": data.get("text"),
                "decode_error": decode_error,
            },
        )

        if assumed_call:
            _LOGGER.info("Предполагаемый звонок в домофон (пуш без data)")
            self._hass.bus.async_fire(EVENT_INCOMING_CALL, {"source": "fcm", "message_id": message_id, "entry_id": self._entry_id, "assumed_call": True, "received_at": now_iso})
            if self.on_call_detected:
                self.on_call_detected()

    async def async_stop(self) -> None:
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None
        if self._fcm_client is not None:
            await self._fcm_client.stop()
        self._set_status("остановлен")
