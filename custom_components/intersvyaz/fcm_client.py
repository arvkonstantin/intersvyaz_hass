"""Локальный subclass firebase-messaging; общая библиотека не патчится."""
from __future__ import annotations

import logging
from firebase_messaging import FcmPushClient

_LOGGER = logging.getLogger(__name__)


def header_value(value: str, name: str) -> str:
    """Выбрать параметр из полного crypto-key/encryption заголовка."""
    parts = [part.strip() for part in value.split(';') if part.strip()]
    prefix = name + '='
    for part in parts:
        if part.startswith(prefix):
            return part[len(prefix):].strip('"')
    return parts[0].strip('"') if parts else ''


def padded(value: str) -> str:
    return value + '=' * (-len(value) % 4)


class IntersvyazFcmPushClient(FcmPushClient):
    """Исправление padding/порядка заголовков только для своего клиента."""

    def _app_data_by_key(self, msg, key, do_not_raise=False):
        value = super()._app_data_by_key(msg, key, do_not_raise)
        if key == 'crypto-key':
            return 'dh=' + header_value(value, 'dh')
        if key == 'encryption':
            return 'salt=' + header_value(value, 'salt')
        return value

    @staticmethod
    def _decrypt_raw_data(credentials, crypto_key_str, salt_str, raw_data):
        try:
            return FcmPushClient._decrypt_raw_data(
                credentials, padded(crypto_key_str), padded(salt_str), raw_data,
            )
        except Exception as err:
            # Повреждённый push не останавливает соединение и не означает звонок.
            _LOGGER.warning('Ошибка расшифровки FCM (%s); сообщение пропущено', type(err).__name__)
            return b'{"_intersvyaz_decode_error":true}'
