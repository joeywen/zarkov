from unittest import TestCase

import ming
import mock

from zarkov import model as M

class TestIdGen(TestCase):

    def setUp(self):
        ming.configure(**{
                'ming.zarkov.master':'mim:///',
                'ming.zarkov.database':'zarkov'})
        M.IdGen.query.remove({})

    def test_get_ids(self):
        ids = M.IdGen.get_ids(100)
        assert len(ids) == 100
        assert ids == range(100), ids
        ids = M.IdGen.get_ids(50)
        assert len(ids) == 50
        assert ids == range(100, 150)

class TestAggDef(TestCase):

    def setUp(self):
        self.ad = M.AggDef(
            name='test',
            task_name='zarkov.tests.test_model.TestTask')
        global TestTask
        self.Task = mock.Mock()
        TestTask = mock.Mock(return_value=self.Task)

    def test_create(self):
        assert self.ad.inputs == [ 'event', 'event.0' ]
        assert not self.ad.realtime
        assert self.ad.last_id == 0

    def test_task(self):
        self.ad.task(limit=100)
        TestTask.assert_called_with(self.ad, limit=100)

    def test_incremental(self):
        sock = mock.Mock()
        self.ad.incremental(sock, 1024)
        TestTask.assert_called_with(self.ad, limit=1024)
        self.Task.run.assert_called_with(sock)

    def test_full(self):
        sock = mock.Mock()
        self.ad.full(sock, 1024)
        TestTask.assert_called_with(self.ad, id_min=0, limit=1024)
        self.Task.run.assert_called_with(sock, full=True)

TestTask = mock.Mock()
