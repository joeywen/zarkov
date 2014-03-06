import logging
import multiprocessing

from setproctitle import setproctitle
from gevent_zeromq import zmq

from .base import Command

log = logging.getLogger(__name__)

class ProxyCommand(Command):
    name='proxy'
    help=("zcmd proxy <bind_addr> <zarkov_addr>\n"
          "Forward 0mq messages from the bind addr to the zarkov addr, \n"
          "validating and stripping a shared key provided on stdin")
    min_args = max_args = 2

    def run(self):
        bind_addr, zark_addr = self.args
        shared_key = raw_input('Shared key:')
        while True:
            p = multiprocessing.Process(
                target=forwarding_proxy,
                args=(bind_addr, zark_addr, shared_key))
            p.daemon = True
            p.start()
            p.join()
            log.info('Subprocess exited with code %d, restarting',
                     p.exitcode)

def forwarding_proxy(bind_addr, zark_addr, shared_key):
    setproctitle('zcmd:proxy child')
    context = zmq.Context()
    incoming = context.socket(zmq.PULL)
    incoming.bind(bind_addr)
    outgoing = context.socket(zmq.PUSH)
    outgoing.connect(zark_addr)
    while True:
        key = incoming.recv()
        more = incoming.getsockopt(zmq.RCVMORE)
        while more:
            part = incoming.recv()
            if key == shared_key:
                log.info('Forward msg')
                outgoing.send(part)
            else:
                log.info('Drop msg')
            more = incoming.getsockopt(zmq.RCVMORE)
