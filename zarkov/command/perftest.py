import time
import logging

import gevent

from zarkov import client
from zarkov import model as ZM
from zarkov import helpers as h
from .base import Command

log = logging.getLogger(__name__)

class PerformanceTestCommand(Command):
    name='perftest'
    help=("pertest <server uri> <n> <c> <r>")
    min_args = max_args = 4

    def run(self):
        n,c,r = map(int, self.args[1:])
        log.info('%d existing events',ZM.event.m.find().count())
        h.setup_time_aggregates()
        ZM.event.m.ensure_indexes()

        begin = time.time()
        jobs = [
            gevent.spawn(_runner, self.args[0], n//c, r/float(c))
            for x in xrange(c) ]
        gevent.joinall(jobs)
        elapsed = time.time() - begin
        log.info('%d events in %.2fs, %.0f r/s' % (
                n, elapsed, float(n) / elapsed))

def _runner(server_uri, n, rate):
    cli = client.ZarkovClient(server_uri)
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
