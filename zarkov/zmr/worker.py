import os
import sys
import zlib
import struct
import logging
import resource
import traceback
import multiprocessing
from operator import itemgetter
from functools import partial
from itertools import groupby

import bson
import gevent
from setproctitle import setproctitle

from gevent_zeromq import zmq

from zarkov import model as ZM
from zarkov import util

log = logging.getLogger(__name__)

class ZMRWorker(object):

    def __init__(self, context, router, src_port, sink_port, suicide_level):
        log.info(
            'Running with %s:(%s,%s,%s)',
            router, src_port, sink_port, suicide_level)
        self._src = context.socket(zmq.PULL)
        self._src.setsockopt(zmq.HWM, 1)
        self._sink = context.socket(zmq.PUSH)
        self._suicide_level = suicide_level
        src_uri = '%s:%s' % (router, src_port)
        sink_uri = '%s:%s' % (router, sink_port)
        self._src.connect(src_uri)
        self._sink.connect(sink_uri)
        def map_keyfunc(t):
            return t[0]
        def xmap_keyfunc(d):
            return d['c'], d['k']
        self._handlers = dict(
            basic=self._handle_basic,
            map=partial(self._handle_maplike, map_keyfunc),
            xmap=partial(self._handle_maplike, xmap_keyfunc),
            reduce=self._handle_reduce,
            xreduce=self._handle_xreduce,
            finalize=self._handle_finalize)

    @classmethod
    def manage_workers(cls, router_addr, nproc):
        def start_proc():
            p = multiprocessing.Process(
                target=cls._worker_proc,
                args=(router_addr,))
            p.daemon = True
            p.start()
            return p
        # Start up processes
        procs = [ start_proc() for i in range(nproc) ]
        # Wait for them to die and restart them
        while True:
            for i, p in enumerate(procs):
                p.join(1)
                if not p.is_alive():
                    if p.exitcode != 0:
                        log.error('Restart failed worker %d with exitcode %d',
                                  p.pid, p.exitcode)
                    procs[i] = start_proc()

    @classmethod
    def _worker_proc(cls, router_addr):
        setproctitle('zmr-worker')
        log.info('zmr-worker startup')
        context = zmq.Context()

        # Get config from router
        sock = context.socket(zmq.REQ)
        sock.connect(router_addr)
        config = util.req_bson(sock, {'$command':'config'})
        host_uri = router_addr.rsplit(':', 1)[0]
        w = cls(
            context,
            host_uri,
            config['src_port'],
            config['sink_port'],
            suicide_level=config.get('suicide_level', 3 * 2**20))
        w.serve_for(config['requests_per_worker_process'])

    def serve_for(self, requests):
        log.debug('Starting worker')
        def rusage_greenlet():
            while True:
                gevent.sleep(60)
                r = resource.getrusage(resource.RUSAGE_SELF)
                log.info('Worker rss %s', r.ru_maxrss)
        _g = gevent.spawn(rusage_greenlet)
        self.run(requests)
        gevent.kill(_g)

    def run(self, requests):
        for req_num in xrange(requests):
            connection = ZM.doc_session.db.connection
            header = util.recv_bson(self._src)
            log.debug('RECV %s', header.keys())
            more = self._src.getsockopt(zmq.RCVMORE)
            parts = []
            while more:
                if header['compress']:
                    parts.append(zlib.decompress(self._src.recv()))
                else:
                    parts.append(self._src.recv())
                more = self._src.getsockopt(zmq.RCVMORE)
            with util.ending_request(connection):
                self.handle_message(header, parts)
            r = resource.getrusage(resource.RUSAGE_SELF)
            if r.ru_maxrss > self._suicide_level:
                log.error('Suicide (painlessly) worker at RSS %s / %s',
                          r.ru_maxrss, self._suicide_level)
                break
        log.info('Closing worker after %d requests', req_num)
        self._src.close()
        self._sink.close()

    def handle_message(self, header, parts):
        jobtype = header['jobtype']
        log.debug('Got message %s', header)
        log.info('Listener %s starting "%s" for job_id %s (size %s)' %
                 (header['listener_id'], jobtype, header['job_id'], len(parts)))
        try:
            handler = self._handlers[jobtype]
            handler(header, parts)
        except:
            log.exception('Error handling message')
            header['error'] = traceback.format_exc()
            util.send_bson(self._sink, header)
        else:
            log.info('Listener %s finished "%s" for job_id %s' %
                     (header['listener_id'], jobtype, header['job_id']))

    def _handle_maplike(self, key, header, parts):
        ns = {}
        exec header['map_text'] in ns
        func = ns[header['map_name']]
        reduce_count = header['reduce_count']
        result = [ [] for x in range(reduce_count) ]
        # Iterate, grouping chunks by the reduce chunk ID
        sz_input = 0
        for part in parts:
            sz_input += len(part)
            for obj in func(util.bson_iter(part)):
                chunk_key = hash(key(obj)) % reduce_count
                result[chunk_key].append(obj)
        assert sz_input, 'There was no input!'
        # Emit reduce chunks one at a time
        util.send_bson(self._sink, header, zmq.SNDMORE)
        for result_chunk in result:
            self._sink.send(
                ''.join(
                    bson.BSON.encode(dict(_id=key, value=value))
                    for key, value in result_chunk),
                zmq.SNDMORE)
        self._sink.send('')

    def _reduce_iter(self, key, parts):
        return bson_groupby(''.join(parts), key)

    def _handle_reduce(self, header, parts):
        ns = {}
        exec header['reduce_text'] in ns
        func = ns[header['reduce_name']]
        result = []
        for key, objs in self._reduce_iter(
            lambda o:o['_id'], parts):
            values = [ o['value'] for o in objs ]
            result.append(dict(
                    _id=key,
                    value=func(key,values)))
        util.send_bson(self._sink, header, zmq.SNDMORE)
        util.send_bson(self._sink, dict(result=result))

    def _handle_xreduce(self, header, parts):
        ns = {}
        exec header['reduce_text'] in ns
        func = ns[header['reduce_name']]
        result = []
        for (c,k), objs in self._reduce_iter(
            lambda o:(o['c'],o['k']), parts):
            values = [ o['v'] for o in objs ]
            result.append(func(c,k,values))
        util.send_bson(self._sink, header, zmq.SNDMORE)
        util.send_bson(self._sink, dict(result=result))

    def _handle_finalize(self, header, parts):
        ns = {}
        exec header['finalize_text'] in ns
        func = ns[header['finalize_name']]
        def obj_iter():
            for part in parts:
                for obj in util.bson_iter(part):
                    yield obj
        result = []
        util.send_bson(self._sink, header, zmq.SNDMORE)
        for result in util.item_chunked_iter(func(obj_iter()), 100):
            sresult = ''.join(map(bson.BSON.encode, result))
            self._sink.send(sresult, zmq.SNDMORE)
        self._sink.send('')

    def _handle_basic(self, header, parts):
        ns = {}
        exec header['func_text'] in ns
        func = ns[header['func_name']]
        try:
            func(*header['args'], **header['kwargs'])
        except:
            for i, line in enumerate(header['func_text'].splitlines()):
                log.info('%.4d: %s', i+1, line)
            raise
        util.send_bson(self._sink, header)

def bson_groupby(idata, key):
    '''Efficient groupy of encoded bson data. This is memory-efficient because it
    only decodes the BSON objects for long enough to get the key value, then it
    discards the decoded object. The sort is a kind of decorate-sort-undecorate
    (DSU) pattern except we use a final groupby() instead of a final undecorate.'''
    unpack = struct.unpack
    BSON = bson.BSON
    index = []
    pos = 0
    ldata = len(idata)
    while pos < ldata:
        sz = unpack('<i', idata[pos:pos+4])[0]
        b = pos
        pos += sz
        k = key(BSON(idata[b:pos]).decode())
        index.append((k, b, pos))
    index.sort()
    for key, group in groupby(index, itemgetter(0)):
        yield key, (
            bson.BSON(idata[b:e]).decode()
            for key,b,e in group )
