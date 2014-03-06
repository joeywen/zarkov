from unittest import TestCase

import ming
import mock
import webtest

from zarkov.command.web_event import WebEventApp

class TestWebEvent(TestCase):

    def setUp(self):
        ming.configure(**{
                'ming.zarkov.master':'mim:///',
                'ming.zarkov.database':'zarkov'})
        self.sock = mock.Mock()
        self.app = webtest.TestApp(WebEventApp(self.sock))

    def test_pixel(self):
        r = self.app.get('/test.gif', status=200)
        assert r.content_type == 'image/gif'
        assert r.content_length == 43
        assert self.sock.send.call_count == 1, self.sock.send.call_count
