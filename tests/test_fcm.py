"""FCM: повреждённые сообщения не означают звонок; библиотека не патчится."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from firebase_messaging import FcmPushClient
from custom_components.intersvyaz.fcm_client import IntersvyazFcmPushClient, header_value, padded
from custom_components.intersvyaz.fcm_listener import FcmCallListener


class FcmClientTests(unittest.TestCase):
    def test_headers_and_padding(self):
        for header, expected in [('dh=abc', 'abc'), ('p256ecdsa=ignored; dh=abc', 'abc'), ('dh="abc"; p256ecdsa=x', 'abc'), ('abc', 'abc')]:
            self.assertEqual(header_value(header, 'dh'), expected)
        self.assertEqual(header_value('rs=4096; salt=xyz', 'salt'), 'xyz')
        self.assertEqual(padded('abc'), 'abc=')

    def test_decode_is_local_and_failure_is_not_empty_push(self):
        base = FcmPushClient._decrypt_raw_data
        self.assertIsNot(IntersvyazFcmPushClient._decrypt_raw_data, base)
        with patch.object(FcmPushClient, '_decrypt_raw_data', return_value=b'{"ok":true}') as decrypt:
            self.assertEqual(IntersvyazFcmPushClient._decrypt_raw_data({}, 'abc', 'xyz', b'data'), b'{"ok":true}')
            decrypt.assert_called_once_with({}, 'abc=', 'xyz=', b'data')
        with patch.object(FcmPushClient, '_decrypt_raw_data', side_effect=ValueError('bad data')):
            result = IntersvyazFcmPushClient._decrypt_raw_data({}, '', '', b'data')
            self.assertIn(b'_intersvyaz_decode_error', result)
        self.assertIs(FcmPushClient._decrypt_raw_data, base)


class FcmListenerTests(unittest.TestCase):
    def setUp(self):
        self.hass = SimpleNamespace(states=Mock(), bus=Mock(), data={})
        with patch('custom_components.intersvyaz.fcm_listener.Store'):
            self.listener = FcmCallListener(self.hass, Mock(), 'test-phone', 'test_entry')
        self.listener.on_call_detected = Mock()

    def test_empty_push_is_explicitly_assumed_and_scoped_to_entry(self):
        self.listener._on_notification({}, 'message-123', None)
        event, data = self.hass.bus.async_fire.call_args.args
        self.assertEqual(event, 'intersvyaz_incoming_call')
        self.assertEqual(data['entry_id'], 'test_entry')
        self.assertEqual(data['message_id'], 'message-123')
        self.assertTrue(data['assumed_call'])

    def test_normal_push_decode_error_and_malformed_do_not_ring(self):
        for obj in [{'data': {'type': 'billing'}}, {'_intersvyaz_decode_error': True}, None, 'not-json']:
            self.listener._on_notification(obj, 'message-123', None)
        self.hass.bus.async_fire.assert_not_called()
        self.listener.on_call_detected.assert_not_called()

    def test_raw_push_and_secrets_are_not_exposed_as_states(self):
        self.listener._on_notification({'data': {'title': 'test', 'token': 'secret'}, 'private': 'secret'}, 'id', None)
        attrs = self.hass.states.async_set.call_args.args[2]
        self.assertNotIn('raw_obj', attrs)
        self.assertNotIn('raw_notification', attrs)
        self.assertNotIn('secret', str(attrs))
