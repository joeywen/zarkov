#!/usr/bin/env python
import resource
import logging
import traceback

import gevent
from gevent_zeromq import zmq

from ming.async import AsyncConnection

from zarkov import util
from zarkov.zmr.job import Job
from zarkov.zmr.job_manager import JobManager

log = logging.getLogger(__name__)

class ZMRRouter(object):
    _instances = []

    def __init__(self, context, options):
        self._instances.append(self)
        self.context = context
        self.options = options
        self.mongodb = AsyncConnection(options.mongo_uri)

        # External sockets
        src_sock = context.socket(zmq.PUSH)
        sink_sock = context.socket(zmq.PULL)
        self._cli_sock = context.socket(zmq.XREP)
        self._cli_sock.bind(options.zmr['req_bind'])

        # Load some options, bind some sockets
        self._uri = options.zmr['worker_uri']
        self._src_port = options.zmr.get('src_port', None)
        self._sink_port = options.zmr.get('sink_port', None)
        if self._src_port:
            src_sock.bind('%s:%s' % (self._uri, self._src_port))
        else:
            self._src_port = src_sock.bind_to_random_port(self._uri)
        if self._sink_port:
            sink_sock.bind('%s:%s' % (self._uri, self._sink_port))
        else:
            self._sink_port = sink_sock.bind_to_random_port(self._uri)

        #Job Manager
        self.job_manager = JobManager(
            src_sock, sink_sock,
            options.zmr['outstanding_chunks'],
            options.zmr['max_chunk_timeout'])

        # Internal request handling sockets
        self._req_worker_sock = context.socket(zmq.XREQ)
        self._req_worker_sock.bind('inproc://req-workers')

        self._terminate = False

    def start(self):
        # gevent.spawn(self.sink_handler)
        for i in xrange(self.options.zmr['request_greenlets']):
            gevent.spawn(self.request_handler, i, self.context, 'inproc://req-workers')
        util.gevent_queue(self._cli_sock, self._req_worker_sock)

    def serve_forever(self):
        while True:
            if self._terminate: break
            gevent.sleep(1)

    def request_handler(self, i, context, uri):
        sock = context.socket(zmq.REP)
        sock.connect(uri)
        while True:
            try:
                obj = util.recv_bson(sock)
                log.debug('Request %s in %s', obj, i)
                command = obj.get('$command')
                log.debug('Req %s in %s', command, i)
                if command == 'echo':
                    util.send_bson(sock, dict(message=obj))
                elif command == 'terminate':
                    self._terminate = True
                elif command in ('mapreduce', 'xmapreduce', 'basic'):
                    job = Job.from_request(self, obj)
                    self.job_manager.alloc_job(job)
                    log.debug('Start job %s', job.id)
                    if obj.get('wait'):
                        job.run()
                        self.job_manager.dealloc_job(job.id)
                    else:
                        gevent.spawn(util.ending_request(self.mongodb), job.run)
                    util.send_bson(sock, dict(job_id=job.id, status=job.status))
                    continue
                elif command == 'status':
                    try:
                        jid = obj['job_id']
                        status = self.job_manager.job_status(jid)
                        util.send_bson(sock, dict(job_id=jid, status=status))
                        if status['state'] in ('complete', 'error'):
                            self.job_manager.dealloc_job(jid)
                    except:
                        log.exception('Error getting status for job')
                        util.send_bson(sock, dict(
                                job_id=obj['job_id'], status=dict(
                                    state='UNKNOWN')))
                    continue
                elif command == 'config':
                    util.send_bson(sock, dict(
                            self.options.zmr,
                            src_port=self._src_port,
                            sink_port=self._sink_port))
            except:
                log.exception('Error in request handler')
                util.send_bson(sock, dict(error=traceback.format_exc()))
                continue
            finally:
                self.mongodb.end_request()
