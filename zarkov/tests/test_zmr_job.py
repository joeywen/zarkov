from nose.tools import assert_equal
from mock import Mock

from zarkov.zmr import job

def test_basicjob():
    j = job.BasicJob(None, None, None, None, [], {})

def test_basicjob_fromrequest_run():
    router = Mock()
    router.job_manager.return_value = [('header', 'content'), ('h2','c2')]
    req = {'$command': 'basic',
           'func_text': 'asdfasdf',
           'func_name': 'blah',
           'args': [],
           'kwargs': {},
          }
    j = job.Job.from_request(router, req)
    assert_equal(type(j), job.BasicJob)

    j.run()
    assert_equal(j.status['state'], 'complete')

def test_basicjob_fromrequest_error_retire():
    router = Mock()
    router.job_manager.side_effect = Exception('explode!')
    req = {'$command': 'basic',
           'func_text': 'asdfasdf',
           'func_name': 'blah',
           'args': [],
           'kwargs': {},
          }
    j = job.Job.from_request(router, req)
    assert_equal(type(j), job.BasicJob)

    j.run()
    assert_equal(j.status['state'], 'error')


def test_mrjob_fromrequest_run():
    router = Mock()
    router.options.zmr = {
        'job_root': '/tmp/zmr-unittest-dir',
        'map_chunk_size': 4,
        'map_chunks_per_page': 2,
    }
    req = {'$command': 'mapreduce',
           'func_text': 'asdfasdf',
           'func_name': 'blah',
           'args': [],
           'kwargs': {},
          }
    j = job.Job.from_request(router, req)
    assert_equal(type(j), job.MRJob)

    j.run() # it errors because we don't mock enough
    assert_equal(j.status['state'], 'error')

