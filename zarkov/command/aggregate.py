import logging

from gevent_zeromq import zmq
from ming.orm import session

from zarkov import model as ZM
from .base import Command

log = logging.getLogger(__name__)

class AggregateCommand(Command):
    name='aggregate'
    help='aggregate <agg name> [ full ]\nRun the specified aggregation'
    min_args = 1
    max_args = 2

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.options.zmr['req_uri'])
        agg = ZM.AggDef.query.get(name=self.args[0])
        log.info('Executing agg: %s', agg.name)
        if len(self.args) > 1:
            agg.full(socket, self.options.zmr['event_limit'])
        else:
            agg.incremental(socket, self.options.zmr['event_limit'])
        session(agg).flush()

