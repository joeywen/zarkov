'''Job manager that includes adaptive timeout and retry for chunks of work.'''
import time
import logging
from Queue import Empty
from contextlib import contextmanager

import bson
import gevent
import gevent.pool
from gevent_zeromq import zmq
from gevent.queue import Queue

from zarkov import util
from zarkov import exceptions as zexc

log = logging.getLogger(__name__)

class JobManager(object):
    _instances = []

    def __init__(self, src_sock, sink_sock, outstanding_chunks, max_timeout=600):
        self._instances.append(self)
        self.jobs = {}
        self._src_sock = src_sock
        self._sink_sock = sink_sock
        self._outstanding_chunks = 0
        self._outstanding_chunk_limit = outstanding_chunks
        self.max_timeout = max_timeout
        gevent.spawn_link_exception(self._g_sink_handler)
        self._pool = gevent.pool.Pool(outstanding_chunks)

    def alloc_job(self, job):
        self.jobs[job.id] = _JobInfo(self, job)

    def dealloc_job(self, job_id):
        log.info('Dealloc job %s', job_id)
        del self.jobs[job_id]

    def job_status(self, job_id):
        return self.jobs[job_id].job.status

    def listener_active(self, job_id, listener_id):
        j = self.jobs[job_id]
        return listener_id in j.listeners

    def __call__(self, job, command, chunk_iter):
        q_greenlet = Queue()
        def g_submitter():
            for args in chunk_iter:
                g = self._pool.spawn_link_exception(
                    self._do_chunk, command, job, *args)
                q_greenlet.put((g, args))
                gevent.sleep(0)
            q_greenlet.put(StopIteration)
        gevent.spawn_link_exception(g_submitter)
        error = None
        for g, args in q_greenlet:
            header, payload = g.get()
            if error:
                continue
            elif 'error' in header:
                log.exception('Got error')
                error = header['error']
            else:
                yield header, payload
        if error:
            raise zexc.WorkerError, error

    def _do_chunk(self, command, job, header, payload_gen=None):
        while self._outstanding_chunks >= self._outstanding_chunk_limit:
            gevent.sleep(0.5)
        self._outstanding_chunks += 1
        try:
            log.debug('Doing chunk %s', job)
            j = self.jobs[job.id]
            if job.status['state'] in ('complete', 'error'):
                log.debug('Skipping chunk on %s job', job.status['state'])
                return None
            while True:
                with  j.alloc_listener(job.id) as (l_queue, lid):
                    try:
                        header['job_id'] = job.id
                        header['listener_id'] = lid
                        begin = time.time()
                        if payload_gen is None:
                            util.send_bson(self._src_sock, header)
                            size = 1
                        else:
                            util.send_bson(self._src_sock, header, zmq.SNDMORE)
                            size = 1 # never let it be 0
                            for payload in payload_gen():
                                self._src_sock.send(payload, zmq.SNDMORE)
                                size += len(payload)
                            self._src_sock.send('')
                        tof = j.timeout_for(command, size)
                        log.info('listener %s: total size of %d kB, time limit %ss',
                                 lid, size >> 10, tof)
                        result = l_queue.get(timeout=tof)
                        t_completed = float(time.time() - begin)
                        j.completed(command, t_completed/size)
                        log.info('listener %s: completed in %ss',
                                 lid, t_completed)
                        log.info('Completed listener %s', lid)
                        return result
                    except Empty:
                        log.info('Timed out listener %s', lid)
                        j.timed_out(command, float(time.time() - begin)/size)
        finally:
            self._outstanding_chunks -= 1

    def _g_sink_handler(self):
        while True:
            header = util.recv_bson(self._sink_sock)
            log.debug('SINK RECV %s', header.keys())
            # Retrieve the payload
            more = self._sink_sock.getsockopt(zmq.RCVMORE)
            parts = []
            while more:
                parts.append(self._sink_sock.recv())
                more = self._sink_sock.getsockopt(zmq.RCVMORE)
            # Grab headers for routing
            job_id = header.pop('job_id', None)
            listener_id = header.pop('listener_id', None)
            # Lookup the job
            j = self.jobs.get(job_id, None)
            if j is None:
                log.error('Discard message for unknown job_id: %s', job_id)
                continue
            # Route the message
            listener = j.listeners.get(listener_id)
            if listener is None:
                log.error('Discard header for unknown listener %s', listener_id)
                continue
            listener.put((header, parts))
                
class _JobInfo(object):
    MIN_TIME_LIMIT=0.1
    DEFAULT_TIME_LIMIT=dict(
        map=0.000005,
        xmap=0.000005,
        reduce=0.000005,
        xreduce=0.000005,
        basic=30)

    def __init__(self, manager, job):
        self.manager = manager
        self.job = job
        self.listeners = {}
        self.time_limit = {}
        self.successful_times = {}

    def timeout_for(self, command, size):
        tl = self.time_limit.setdefault(
            command, self.DEFAULT_TIME_LIMIT[command])
        raw_time_limit = max(size * tl, self.MIN_TIME_LIMIT)
        return min(raw_time_limit, self.manager.max_timeout)

    def timed_out(self, command, seconds):
        self.time_limit[command] = tl = max(
            self.DEFAULT_TIME_LIMIT[command], seconds * 2)
        log.info('... timed out %s in %ss, new tl = %s', command, seconds, tl)

    def completed(self, command, seconds):
        log.debug('Completed %s in %ss', command, seconds)
        l = self.successful_times.setdefault(command, [])
        l.append(seconds)
        if len(l) > 5:
            tl = 5.0 * sum(l) / len(l)
            tl = max(tl, self.MIN_TIME_LIMIT)
            self.time_limit[command] = tl
            log.debug('... new time limit is %ss', tl)
            l = l[-5:]
        current_tl = self.time_limit.get(command)
        new_tl = seconds * 10
        if new_tl > current_tl:
            self.time_limit[command] = new_tl

    @contextmanager
    def alloc_listener(self, job_id):
        q = Queue()
        lid = '%s:%s' % (job_id, bson.ObjectId())
        self.listeners[lid] = q
        yield q, lid
        del self.listeners[lid]
