'''Python client for zarkov.'''
import zmq

import bson

class ZarkovClient(object):

    def __init__(self, addr):
        context = zmq.Context.instance()
        self._sock = context.socket(zmq.PUSH)
        self._sock.connect(addr)

    def event(self, type, context, extra=None):
        obj = dict(
            type=type, context=context, extra=extra)
        self._sock.send(bson.BSON.encode(obj))

    def event_noval(self, type, context, extra=None):
        from zarkov import model
        obj = model.event.make(dict(
                type=type,
                context=context,
                extra=extra))
        obj['$command'] = 'event_noval'
        self._sock.send(bson.BSON.encode(obj))

    def _command(self, cmd, **kw):
        d = dict(kw)
        d['$command'] = cmd
        self._sock.send(bson.BSON.encode(d))
