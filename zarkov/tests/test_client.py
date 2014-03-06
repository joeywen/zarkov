from unittest import TestCase

import ming
import mock
import webtest
import bson

from zarkov.client import ZarkovClient

class TestClient(TestCase):

    def setUp(self):
        ming.configure(**{
                'ming.zarkov.master':'mim:///',
                'ming.zarkov.database':'zarkov'})
        self.cli = ZarkovClient('tcp://localhost:6543')
        self.cli._sock = mock.Mock()

    def test_event_noval(self):
        self.cli.event_noval('nop', {'sample_context_key': 'sample_context_val'})
        assert self.cli._sock.send.call_count == 1
        sent = bson.BSON.decode(self.cli._sock.send.call_args[0][0])
        assert 'timestamp' in sent
        assert sent['$command'] == 'event_noval'
        assert sent['context'] == dict(sample_context_key='sample_context_val')
                
    def test_event(self):
        self.cli.event('nop', {'sample_context_key': 'sample_context_val'})
        assert self.cli._sock.send.call_count == 1
        assert bson.BSON.decode(self.cli._sock.send.call_args[0][0]) == dict(
            type='nop',
            extra=None,
            context=dict(
                sample_context_key='sample_context_val'))