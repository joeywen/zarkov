import os
import logging
import resource
import multiprocessing

import bson
import gevent
from gevent_zeromq import zmq
from setproctitle import setproctitle

from zarkov import journal, model, util
from .base import Command

log = logging.getLogger(__name__)

HWM=10000
HWM=20

class EventLogCommand(Command):
    name='eventlog'
    help=("Run the Zarkov event logger")

    min_args=0
    max_args=None

    def run(self):
        import faulthandler
        faulthandler.enable()
        
        setproctitle('zcmd:eventlog(m)')

        # Start workers
        nproc = self.options.num_event_servers or multiprocessing.cpu_count()
        procs = []
        for i in range(nproc):
            pipe = os.pipe()
            proc = multiprocessing.Process(
                target=event_logger, args=(self.options, i, pipe))
            procs.append((pipe, proc))
        for pipe, proc in procs:
            proc.daemon = True
            proc.start()

        # Get zmq context
        context = zmq.Context()
        s_outgoing = context.socket(zmq.PUSH)
        # s_outgoing.setsockopt(zmq.HWM, HWM)
        p_outgoing = s_outgoing.bind_to_random_port('tcp://0.0.0.0')
        log.info('event_logger router port: %r', p_outgoing)
        for pipe, proc in procs:
            os.write(pipe[1], str(p_outgoing))

        # Forward events from incoming socket to loggers, converting json to bson
        # where necessary
        def bson_server():
            # Wait on messages
            s_bson = context.socket(zmq.PULL)
            s_bson.setsockopt(zmq.HWM, HWM)
            log.info('Listening for bson events at %s' % self.options.bson_bind_address)
            s_bson.bind(self.options.bson_bind_address)
            while True:
                msg = s_bson.recv(copy=False)
                s_outgoing.send(msg, copy=False)
        def json_server():
            # Wait on messages
            s_json = context.socket(zmq.PULL)
            s_json.setsockopt(zmq.HWM, HWM)
            log.info('Listening for json events at %s' % self.options.json_bind_address)
            s_json.bind(self.options.json_bind_address)
            while True:
                j_msg = s_json.recv(copy=False)
                b_msg = zmq.Message(util.bson_from_json(j_msg.buffer))
                s_outgoing.send(b_msg, copy=False)
        gevent.spawn_link_exception(bson_server)
        gevent.spawn_link_exception(json_server)

        # Print resource usage every 10s
        while True:
            for pipe, proc in procs:
                if not proc.is_alive():
                    log.error('found dead process; cleaning up. %s' % (proc,))
                    proc.terminate()
            if not any(proc.is_alive() for pipe, proc in procs):
                raise Exception('All child processes are dead')

            rs = resource.getrusage(resource.RUSAGE_SELF)
            rc = resource.getrusage(resource.RUSAGE_CHILDREN)
            log.info('Max resident set size: self %s children %s' % (rs.ru_maxrss, rc.ru_maxrss))
            gevent.sleep(60)

def event_logger(options, proc_num, pipe):
    p_upstream = int(os.read(pipe[0], 100))
    log.info('event_logger daemon port: %s', p_upstream)
    setproctitle('zcmd:eventlog(w)')
    context = zmq.Context()
    s_upstream = context.socket(zmq.PULL)
    # s_upstream.setsockopt(zmq.HWM, HWM)
    s_upstream.connect('tcp://localhost:' + str(p_upstream))

    # Start event logger
    journal_dir = '%s/%s' % (options.journal, proc_num)
    log.info('Journaling to %s', journal_dir)
    if not os.path.exists(journal_dir):
        os.makedirs(journal_dir)
    j = journal.JournalWriter(options, journal_dir)

    # Handle messages
    while True:
        msg = s_upstream.recv()
        obj = bson.BSON(msg).decode()
        command = obj.pop('$command', None)
        if command != 'event_noval':
            obj = model.event(obj).make(obj)
        j(obj)
