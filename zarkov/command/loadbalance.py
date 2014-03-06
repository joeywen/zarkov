import logging

from gevent_zeromq import zmq

from .base import Command

log = logging.getLogger(__name__)

HWM=1000

class LoadBalanceCommand(Command):
    name='loadbalance'
    help=("Zarkove event load balancer")
    
    min_args=0
    max_args=0

    def run(self):
        context = zmq.Context()
        incoming_sock = context.socket(zmq.PULL)
        incoming_sock.bind(self.options.loadbalance['incoming_bind'])

        outgoing_sockets = []
        for uri in self.options.loadbalance['outgoing_uris']:
            sock = context.socket(zmq.PUSH)
            sock.connect(uri)
            outgoing_sockets.append(sock)

        while True:
            for outgoing_socket in outgoing_sockets:
                msg = incoming_sock.recv()
                outgoing_socket.send(msg)
