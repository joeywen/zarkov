from nose.tools import assert_equal, with_setup
from bson import BSON
from collections import namedtuple

from ming import mim

from zarkov import task
from zarkov.model import AggDef

class TestTask(task.Task):
    def _start_jobs(*a, **kw):
        pass
aggdef = AggDef()

class Socket(object):
    def __init__(self):
        self.sent = []
    def send(self, msg, *a, **kw):
        self.sent.append(msg)
    def recv(self):
        return BSON.encode({'job_id': 'foobar'})

def set_up():
    mim.Connection.get().drop_all()

@with_setup(set_up)
def test_task():
    t = TestTask(aggdef)
    progress = t.run(socket=None)
    assert_equal(progress, 0)

@with_setup(set_up)
def test_task_limit():
    t = TestTask(aggdef, limit=2)
    progress = t.run(socket=None)
    assert_equal(progress, 0)

@with_setup(set_up)
def test_task_limit_full():
    t = TestTask(aggdef, limit=2)
    progress = t.run(socket=None, full=True)
    assert_equal(progress, 0)

@with_setup(set_up)
def test_task_basic():
    t = TestTask(aggdef)
    socket = Socket()
    def func():
        return 'asdf'
    result = t.start_basic_job(socket, func)
    assert_equal(result, {'job_id': 'foobar'})
    sent = BSON.decode(socket.sent[0])
    assert_equal(sent['$command'], 'basic')
    assert_equal(sent['func_name'], 'func')
    assert "return 'asdf'" in sent['func_text']

@with_setup(set_up)
def test_task_mr():
    t = TestTask(aggdef)
    socket = Socket()
    Collection = namedtuple('Collection', 'name')
    result = t.start_mr(socket, full=False, collection=Collection('zagg.foo'))
    assert_equal(result, {'job_id': 'foobar'})
    sent = BSON.decode(socket.sent[0])
    assert_equal(sent['$command'], 'mapreduce')
    assert_equal(sent['output_type'], 'reduce')
    assert_equal(sent['output_name'], 'zagg.foo')

@with_setup(set_up)
def test_task_mr_more():
    t = TestTask(aggdef)
    socket = Socket()
    Collection = namedtuple('Collection', 'name')
    def map_func():
        pass
    def reduce_func():
        pass
    def finalize_func():
        pass
    result = t.start_mr(socket, full=True, collection=Collection('zagg.foo'),
                        map=map_func, reduce=reduce_func, finalize=finalize_func)
    assert_equal(result, {'job_id': 'foobar'})
    sent = BSON.decode(socket.sent[0])
    assert_equal(sent['$command'], 'mapreduce')
    assert_equal(sent['output_type'], 'replace')
    assert_equal(sent['output_name'], 'zagg.foo')

@with_setup(set_up)
def test_task_xmr():
    t = TestTask(aggdef)
    socket = Socket()
    result = t.start_xmr(socket, full=False)
    assert_equal(result, {'job_id': 'foobar'})
    sent = BSON.decode(socket.sent[0])
    assert_equal(sent['$command'], 'xmapreduce')

@with_setup(set_up)
def test_task_xmr_more():
    t = TestTask(aggdef)
    socket = Socket()
    def map_func():
        pass
    def reduce_func():
        pass
    def finalize_func():
        pass
    result = t.start_xmr(socket, full=True, map=map_func, reduce=reduce_func, finalize=finalize_func)
    assert_equal(result, {'job_id': 'foobar'})
    sent = BSON.decode(socket.sent[0])
    assert_equal(sent['$command'], 'xmapreduce')
    assert_equal(sent['output_type'], 'replace')
    assert_equal(sent['output_name'], 'zagg.result')

