'''Simple performance test to size up zarkov'''

import sys
import time
import logging
from contextlib import contextmanager 

import gevent

logging.basicConfig()
logging.Formatter.converter=time.gmtime

from zarkov import model
from zarkov import config
from zarkov import helpers as h
from zarkov.client import ZarkovClient

log = logging.getLogger(__name__)

n=c=r=None

def run(argv):
    global n,c,r
    n = int(argv[1])
    c = int(argv[2])
    r = 600

    config.configure()
    try:
        print '%d existing events' % (
            model.event.m.find().count())

        # h.setup_time_aggregates()
        model.event.m.ensure_indexes()
    except:
        log.exception('Error initializing mongo')

    jobs = [
        gevent.spawn(runner, n//c, r/float(c))
        for x in xrange(c) ]
    with timing() as t:
        gevent.joinall(jobs)

    print '%d events in %.2f s, %.0f r/s' % (
        n, t[0], float(n)/(t[0]))
    return

    agg_defs = model.AggDef.query.find().all()
    for ad in agg_defs:
        print 
        print '=== Aggregate:', ad.name, '==='
        # updated = ad.incremental()
        # updated = ad.full()
        for doc in ad.collection.find().sort('_id'):
            print '%8s: %s' % (doc['_id'], doc['value'])

def runner(n, rate):
    cli = ZarkovClient('tcp://localhost:6543')
    tm_b = time.time()
    for chunk in xrange(n):
        for y in xrange(int(rate+1)):
            if n <= 0: return
            n -= 1
            cli.event_noval(
                type='foo',
                context=dict(
                    neighborhood='projects',
                    project='test',
                    tool='wiki',
                    app='home',
                    user='admin1'))
        to_wait = tm_b +  chunk + 1 - time.time()
        if to_wait > 0:
            gevent.sleep(to_wait)

@contextmanager
def timing():
    timer = [0]
    start = time.time()
    yield timer
    stop = time.time()
    timer[0] = stop-start

if __name__ == '__main__':
    run(sys.argv)
