import logging
import resource

import bson
import gevent
import pkg_resources
from gevent.server import StreamServer
from gevent.pool import Pool
from gevent_zeromq import zmq

from .base import Command

log = logging.getLogger(__name__)

HWM=1000

class LogStreamCommand(Command):
    name='logstream'
    help=("logstream <eventlog_uri> <plugin>:<port> ...")
    detailed_help=(
          "Listens on a given port for line-oriented data, applies a\n"
          "plugin transform to convert each line to an event, and saves the\n"
          "event to Zarokv")
    
    min_args=0
    max_args=0

    def run(self):
        context = zmq.Context()
        sock = context.socket(zmq.PUSH)
        sock.setsockopt(zmq.HWM, HWM)
        sock.connect(self.options.bson_uri)

        for plugin in getattr(self.options, 'logstream', []):
            for ep in pkg_resources.iter_entry_points('zarkov.logstream', plugin['entry_point']):
                StreamPlugin(sock, plugin['port'], ep.load()).start()
                break
            else:
                raise RuntimeError, 'Unknown entry point %s' % plugin['entry_point']

        # Print resource usage every 10s
        while True:
            rc = resource.getrusage(resource.RUSAGE_SELF)
            rs = resource.getrusage(resource.RUSAGE_CHILDREN)
            log.info('RSS: %s', rc.ru_maxrss + rs.ru_maxrss)
            gevent.sleep(10)

class StreamPlugin(object):
    HWM=1000

    def __init__(self, socket, port, make_events):
        self.socket = socket
        self.port = port
        self.make_events = make_events

    def __repr__(self):
        return '<StreamPlugin %s:%s>' % (
            self.port, self.make_events.__doc__ or self.make_events)

    def start(self):
        log.info('%r listening', self)
        pool = Pool(1000)
        self.server = StreamServer(
            ('0.0.0.0', self.port),
            handle=self.handle,
            spawn=pool)
        self.server.start()

    def handle(self, socket, address):
        log.info('%r received connection from %s', self, address)
        for ev in self.make_events(socket.makefile()):
            self.socket.send(bson.BSON.encode(ev))
            gevent.sleep(0)
            
        
