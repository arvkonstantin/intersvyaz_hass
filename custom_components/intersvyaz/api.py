"""Единый асинхронный клиент для API Интерсвязь.

Раньше запросы к api.is74.ru и cams.is74.ru были продублированы в
__init__.py и в config_flow.py (с разным, местами несовместимым разбором
ответа — например get_relay_id в двух файлах ожидал разные форматы JSON).
Этот модуль — единственное место, где происходит обращение к сети, что
упрощает сопровождение и тестирование.
"""
from __future__ import annotations

import logging
import uuid as uuid_lib
from typing import Any, Optional

import aiohttp

_LOGGER = logging.getLogger(__name__)

BASE_URL = "https://api.is74.ru"
BASE_URL_CAM = "https://cams.is74.ru"
BASE_URL_CDN = "https://cdn.cams.is74.ru"
BASE_URL_TD_CRM = "https://td-crm.is74.ru/api"

API_TIMEOUT = aiohttp.ClientTimeout(total=15)


class IntersvyazApiError(Exception):
    """Базовая ошибка при обращении к API Интерсвязь."""


class IntersvyazAuthError(IntersvyazApiError):
    """Токен недействителен/истёк или неверные учётные данные."""


class IntersvyazApiClient:
    """Тонкая обёртка над мобильным API Интерсвязь."""

    def __init__(self, session: aiohttp.ClientSession, token: Optional[str] = None) -> None:
        self._session = session
        self.token = token

    def _auth_headers(self) -> dict[str, str]:
        if not self.token:
            raise IntersvyazAuthError("Токен не установлен")
        return {"Authorization": f"Bearer {self.token}"}

    async def _request(
        self, method: str, url: str, *, auth: bool = False, **kwargs: Any
    ) -> Any:
        headers = kwargs.pop("headers", {}) or {}
        if auth:
            headers.update(self._auth_headers())

        try:
            async with self._session.request(
                method, url, headers=headers, timeout=API_TIMEOUT, **kwargs
            ) as resp:
                text = await resp.text()

                if resp.status in (401, 403):
                    raise IntersvyazAuthError(
                        f"Ошибка авторизации ({resp.status}) при запросе {url}"
                    )
                if not (200 <= resp.status < 300):
                    _LOGGER.debug("Ответ HTTP %s на %s", resp.status, url)
                    raise IntersvyazApiError(
                        f"HTTP {resp.status} при запросе {url}"
                    )

                if not text:
                    return None
                try:
                    return await resp.json(content_type=None)
                except ValueError as err:
                    raise IntersvyazApiError(
                        f"Не удалось разобрать ответ сервера ({url}): {err}"
                    ) from err
        except aiohttp.ClientError as err:
            raise IntersvyazApiError(f"Ошибка сети при запросе {url}: {err}") from err
        except TimeoutError as err:
            raise IntersvyazApiError(f"Таймаут при запросе {url}") from err

    # ------------------------------------------------------------------ #
    # Авторизация по логину/паролю
    # ------------------------------------------------------------------ #
    async def login(self, username: str, password: str) -> str:
        data = await self._request(
            "POST",
            f"{BASE_URL}/auth/mobile",
            json={"username": username, "password": password},
            headers={"Content-Type": "application/json"},
        )
        token = data.get("TOKEN") if data else None
        if not token:
            raise IntersvyazAuthError("Токен отсутствует в ответе сервера")
        self.token = token
        return token

    # ------------------------------------------------------------------ #
    # Авторизация по номеру телефона (SMS)
    # ------------------------------------------------------------------ #
    async def send_sms(self, phone: str) -> str:
        """Отправляет СМС с кодом, возвращает device_id для следующих шагов."""
        device_id = uuid_lib.uuid4().hex
        await self._request(
            "POST",
            f"{BASE_URL}/mobile/auth/send-sms",
            json={"phone": phone.replace("+7", ""), "uniqueDeviceId": device_id},
        )
        return device_id

    async def confirm_sms(self, phone: str, code: str, device_id: str) -> dict:
        data = await self._request(
            "POST",
            f"{BASE_URL}/mobile/auth/confirm",
            json={
                "confirmCode": code,
                "phone": phone.replace("+7", ""),
                "uniqueDeviceId": device_id,
            },
        )
        if not data or "authId" not in data or "addresses" not in data:
            raise IntersvyazApiError("Неверный код подтверждения")
        return data

    async def token_by_address(self, auth_id: str, user_id: str) -> str:
        data = await self._request(
            "POST",
            f"{BASE_URL}/mobile/auth/get-token",
            json={"authId": str(auth_id).strip(), "userId": str(user_id).strip()},
            headers={"Content-Type": "application/json"},
        )
        token = (data.get("TOKEN") or data.get("token")) if data else None
        if not token:
            raise IntersvyazAuthError("Токен отсутствует в ответе сервера")
        self.token = token
        return token

    # ------------------------------------------------------------------ #
    # Домофон / реле
    # ------------------------------------------------------------------ #
    async def get_relays(self) -> list[dict]:
        """Вернуть все доступные реле с уникальными нормализованными ID."""
        data = await self._request("GET", f"{BASE_URL}/domofon/relays", auth=True)
        if not isinstance(data, list):
            raise IntersvyazApiError("API не вернул список реле")

        relays: list[dict] = []
        seen: set[str] = set()
        for relay in data:
            if not isinstance(relay, dict):
                continue
            value = relay.get("RELAY_ID")
            if isinstance(value, bool) or not isinstance(value, (str, int)):
                continue
            relay_id = str(value).strip()
            if not relay_id.isascii() or not relay_id.isdecimal() or relay_id in seen:
                continue
            seen.add(relay_id)
            relays.append({**relay, "RELAY_ID": relay_id})
        return relays

    async def get_relay_id(self) -> str:
        data = await self._request("GET", f"{BASE_URL}/domofon/relays", auth=True)
        if isinstance(data, list) and data:
            relay_id = data[0].get("RELAY_ID")
            if relay_id:
                return relay_id
        raise IntersvyazApiError("API не вернул список реле")

    async def open_door(self, relay_id: str) -> None:
        await self._request(
            "POST",
            f"{BASE_URL}/domofon/relays/{relay_id}/open?from=app",
            auth=True,
            headers={"Content-Type": "application/json"},
        )

    # ------------------------------------------------------------------ #
    # Push-токен (регистрация "виртуального устройства" для приёма
    # data-пушей о звонках в домофон через FCM — см. fcm_listener.py)
    # ------------------------------------------------------------------ #
    async def register_push_token(
        self,
        fcm_token: str,
        unique_device_id: str,
        authorize_phone: str,
        first_sim_phone: str | None = None,
        version_name: str = "2.20.0",
        assembly_code: int = 2026081813,
        os_version: str = "14",
        device_model: str = "Home Assistant",
    ) -> str:
        """Регистрирует FCM push-токен на бэкенде api.is74.ru — сервер
        начинает слать на него data-пуши, как для настоящего Android-
        приложения. Возвращает ID регистрации устройства."""
        data = await self._request(
            "PUT",
            f"{BASE_URL}/mobile/pushtoken/add-with-device-id",
            auth=True,
            json={
                "TOKEN": fcm_token,
                "TYPE": 3,  # TOKEN_TYPE_FIREBASE
                "UNIQUE_DEVICE_ID": unique_device_id,
                "FIRST_SIM_PHONE": first_sim_phone.replace("+7", "") if first_sim_phone else None,
                "AUTHORIZE_PHONE": authorize_phone.replace("+7", ""),
                "VERS_NAME": version_name,
                "ASSEMBLY_CODE": assembly_code,
                "OS_VERS": os_version,
                "DEVICE_MODEL": device_model,
            },
            headers={"Content-Type": "application/json"},
        )
        if not data or "ID" not in data:
            raise IntersvyazApiError("Сервер не вернул ID регистрации устройства")
        return data["ID"]

    # ------------------------------------------------------------------ #
    # td-crm.is74.ru — отдельный бэкенд, регистрирует то же "виртуальное
    # устройство" во ВТОРОМ списке push-получателей. Судя по практике,
    # именно эта регистрация (а не register_push_token выше) реально
    # нужна для получения пушей о звонках в домофон.
    # ------------------------------------------------------------------ #
    TD_BUYER_ID = 1  # захардкоженная константа во всех версиях приложения

    async def check_td_token(self, td_token: str) -> bool:
        """GET auth на td-crm.is74.ru — проверяет, жив ли уже имеющийся
        td-токен, не тратя новую сессию через auth_lk()."""
        try:
            await self._request(
                "GET",
                f"{BASE_URL_TD_CRM}/auth",
                headers={"Authorization": f"Bearer {td_token}"},
            )
            return True
        except (IntersvyazApiError, IntersvyazAuthError):
            return False

    async def auth_lk(self) -> dict:
        """Обменивает текущий токен api.is74.ru на отдельный токен td-crm
        (buyerId всегда 1, см. TD_BUYER_ID)."""
        if not self.token:
            raise IntersvyazAuthError("Токен api.is74.ru не установлен")
        data = await self._request(
            "POST",
            f"{BASE_URL_TD_CRM}/auth-lk",
            data={"token": self.token, "buyerId": str(self.TD_BUYER_ID)},
        )
        if not data or "TOKEN" not in data:
            raise IntersvyazApiError("td-crm не вернул TOKEN в ответе auth-lk")
        return data

    async def register_td_device(
        self,
        td_token: str,
        *,
        app_id: str,
        push_token: str,
        platform: str,
        device_name: str,
        device_id: str,
        sending_push: bool = True,
        notification_type: str = "push",
    ) -> None:
        """PUT user-device на td-crm.is74.ru — регистрация устройства для
        получения push-уведомлений о звонках. Схема подтверждена перехватом
        реального трафика официального приложения (appId — package name,
        notificationType="push" обязателен)."""
        await self._request(
            "PUT",
            f"{BASE_URL_TD_CRM}/user-device",
            headers={"Authorization": f"Bearer {td_token}", "Content-Type": "application/json"},
            json={
                "appId": app_id,
                "pushToken": push_token,
                "platform": platform,
                "deviceName": device_name,
                "deviceId": device_id,
                "sendingPush": sending_push,
                "notificationType": notification_type,
            },
        )

    async def get_device_settings(self, td_token: str, device_id: str, app_id: str) -> dict:
        """GET v1/user-device/{deviceId}/{appId}/info на td-crm.is74.ru —
        как СЕРВЕР видит нашу текущую регистрацию (диагностика)."""
        data = await self._request(
            "GET",
            f"{BASE_URL_TD_CRM}/v1/user-device/{device_id}/{app_id}/info",
            headers={"Authorization": f"Bearer {td_token}"},
        )
        return data or {}

    # ------------------------------------------------------------------ #
    # Камеры
    # ------------------------------------------------------------------ #
    async def get_group_id(self) -> str:
        """ID группы 'Умный двор', либо, если её нет, 'Свои камеры'."""
        data = await self._request("GET", f"{BASE_URL_CAM}/api/get-group/", auth=True)
        for group in data or []:
            if group.get("NAME", "").startswith("Умный двор"):
                return group["ID"]

        data = await self._request(
            "GET", f"{BASE_URL_CAM}/api/get-group/?selfCams=true", auth=True
        )
        for group in data or []:
            if group.get("NAME") == "Свои камеры":
                return group["ID"]

        raise IntersvyazApiError(
            "Не найдены ни группа 'Умный двор', ни 'Свои камеры'"
        )

    async def get_group_cameras(self, group_id: str) -> list[dict]:
        data = await self._request(
            "GET", f"{BASE_URL_CAM}/api/get-group/{group_id}", auth=True
        )
        if not isinstance(data, list):
            raise IntersvyazApiError(f"Неожиданный формат данных камер: {type(data)}")
        return data

    def snapshot_url(self, camera_uuid: str) -> str:
        return f"{BASE_URL_CDN}/snapshot?uuid={camera_uuid}&token=bearer-{self.token}"

    def fallback_stream_url(self, camera_uuid: str) -> str:
        return (
            f"{BASE_URL_CDN}/hls/playlists/multivariant.m3u8"
            f"?uuid={camera_uuid}&token=bearer-{self.token}"
        )