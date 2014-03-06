from nose.tools import assert_equal
from mock import Mock

from zarkov.zmr import job_manager

def test_jobinfo():
    mgr = job_manager.JobManager(None, None, None)
    ji = job_manager._JobInfo(mgr, None)
    assert_equal(ji.timeout_for('map', 123), ji.MIN_TIME_LIMIT)

    ji.timed_out('map', 12)
    assert_equal(ji.time_limit, {'map': 24}) # doubled

    ji.completed('map', 42)
    assert_equal(ji.time_limit, {'map': 420}) # 10x

    for i in xrange(1, 12): # to exercise the truncator
        ji.completed('map', 4)
    assert_equal(ji.time_limit, {'map': 40}) # 10x
