from unittest import TestCase

import ming
import mock
import webtest

from zarkov.web import WebApp

from zarkov import model as M

class TestWeb(TestCase):
    def setUp(self):
        ming.configure(**{
                'ming.zarkov.master':'mim:///',
                'ming.zarkov.database':'zarkov'})
        self.app = webtest.TestApp(WebApp())

    def test_query(self):
        M.AggDef(name='test',task_name='zarkov.tests.test_model.TestTask')
        r = self.app.post('/q',params={'data':'{"c":"test"}'})
        assert r.body == '{"data": {}}'

    def test_bad_params(self):
        self.app.post('/q',params={'data':'{"z":"test"}'}, status=400)