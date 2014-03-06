import os
import shutil
import tempfile
from unittest import TestCase

import ming
import mock

from zarkov import model as ZM
from zarkov.journal import JournalWriter

class TestEventLog(TestCase):

    def setUp(self):
        ming.configure(**{
                'ming.zarkov.master':'mim:///',
                'ming.zarkov.database':'zarkov'})
        self.options = mock.Mock()
        self.options.journal_file_size = 1024
        self.options.journal_min_files = 4
        self.journal_dir = tempfile.mkdtemp('.journal', 'zarkov-test')
        self.j = JournalWriter(self.options, self.journal_dir, start_greenlets=False)

    def tearDown(self):
        shutil.rmtree(self.journal_dir)

    def test_create_dirs(self):
        fns = os.listdir(self.journal_dir)
        assert len(fns) == 4

    def test_handle_events(self):
        for i in range(100):
            self.j(dict(a=5, b=6, i=i))
        self.j._enqueue_commit()
        self.j._commit_queue.put(StopIteration)
        self.j._commit_greenlet()
        assert ZM.event.m.find().count() == 100, ZM.event.m.find().count()
        assert ZM.idgen.m.find().next() == dict(_id=0, inc=100)
