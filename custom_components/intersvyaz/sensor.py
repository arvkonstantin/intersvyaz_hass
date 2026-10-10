"""Sensor platform for Intersvyaz Domofon integration.

Единственная сущность здесь — индикатор "Звонок", видимый прямо на
странице интеграции . Остальная FCM-диагностика (sensor.intersvyaz_fcm_status_*, sensor.intersvyaz_last_push_*)
пишется напрямую через hass.states.async_set() в fcm_listener.py
"""
from __future__ import annotations

import logging

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.event import async_call_later

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

CALL_INDICATOR_RESET_SECONDS = 5


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities
) -> None:
    """Один индикатор звонка на запись конфигурации (привязан к первому/
    основному адресу — звонок в домофон один на весь аккаунт, а не на
    каждый отдельный адрес)."""
    accounts: list[dict] = hass.data[DOMAIN][entry.entry_id]["accounts"]
    if not accounts:
        return

    primary = accounts[0]
    entity = IntersvyazCallIndicator(
        hass,
        entry.entry_id,
        primary["device_identifier"],
        primary["address"],
        f"call_indicator_{entry.entry_id}",
    )
    async_add_entities([entity])


class IntersvyazCallIndicator(SensorEntity):
    """Показывает 'Звонок' на CALL_INDICATOR_RESET_SECONDS секунд при
    предполагаемом звонке в домофон, затем сама сбрасывается в 'Нет'."""

    _attr_has_entity_name = True
    _attr_icon = "mdi:phone-ring"

    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str,
        device_identifier: tuple[str, str],
        address: str,
        unique_id: str,
    ) -> None:
        self.hass = hass
        self._entry_id = entry_id
        self._attr_name = "Возможный звонок"
        self._attr_unique_id = unique_id
        self._attr_native_value = "Нет"
        self._attr_device_info = DeviceInfo(
            identifiers={device_identifier},
            name=address,
            manufacturer="Интерсвязь",
            model="Домофон IS74",
        )
        self._reset_unsub = None

    async def async_added_to_hass(self) -> None:
        """Регистрируем себя в hass.data, чтобы fcm_listener.py мог найти
        эту сущность и дёрнуть trigger_call() при предполагаемом звонке —
        без прямой связи между модулями, устойчиво к порядку запуска."""
        self.hass.data.setdefault(DOMAIN, {}).setdefault(self._entry_id, {})
        self.hass.data[DOMAIN][self._entry_id]["call_indicator_entity"] = self

    async def async_will_remove_from_hass(self) -> None:
        entry_data = self.hass.data.get(DOMAIN, {}).get(self._entry_id, {})
        if entry_data.get("call_indicator_entity") is self:
            entry_data.pop("call_indicator_entity", None)
        if self._reset_unsub:
            self._reset_unsub()
            self._reset_unsub = None

    @callback
    def trigger_call(self) -> None:
        """Вызывается из fcm_listener.py при предполагаемом звонке."""
        self._attr_native_value = "Звонок?"
        self.async_write_ha_state()

        if self._reset_unsub:
            self._reset_unsub()

        self._reset_unsub = async_call_later(
            self.hass, CALL_INDICATOR_RESET_SECONDS, self._reset
        )

    @callback
    def _reset(self, _now) -> None:
        self._attr_native_value = "Нет"
        self._reset_unsub = None
        self.async_write_ha_state()