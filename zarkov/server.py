'''This is the main  zarkov event server. It uses gevent for a fast event loop in
a single thread. It should typically be invoked with the zarkov-server script.'''

import sys
import time
import logging
from datetime import datetime, timedelta

import bson
import gevent
from gevent_zeromq import zmq

from zarkov import model
from zarkov import util

log = logging.getLogger(__name__)

class Server(object):

    def __init__(self, context, options, j):
        self.context = context
        self._options = options
        self._j = j
        self.handlers = {
            'event_noval':self._handle_event_noval, # log an event with no validation
            None:self._handle_event} # log an event with validation
        if options.publish_bind_address:
            self._pub_sock = context.socket(zmq.PUB)
            self._pub_sock.bind(options.publish_bind_address)
        else:
            self._pub_sock = None

    def serve_forever(self):
        q = gevent.queue.Queue(2**10)

        def bson_server():
            # Wait on messages
            s_bson = self.context.socket(zmq.PULL)
            s_bson.bind(self._options.bson_bind_address)
            while True:
                q.put(s_bson.recv())
        def json_server():
            # Wait on messages
            s_json = self.context.socket(zmq.PULL)
            s_json.bind(self._options.json_bind_address)
            while True:
                msg = s_json.recv()
                q.put(util.bson_from_json(msg))
        if self._options.bson_bind_address:
            gevent.spawn(bson_server)
        if self._options.json_bind_address:
            gevent.spawn(json_server)
        if self._options.incremental:
            gevent.spawn(self._g_aggregate)

        def resource_print():
            import resource
            while True:
                rc = resource.getrusage(resource.RUSAGE_SELF)
                rs = resource.getrusage(resource.RUSAGE_CHILDREN)
                log.info('Server rss %s', rc.ru_maxrss + rs.ru_maxrss)
                gevent.sleep(10)
        gevent.spawn(resource_print)


        log.info('Starting main server on %s / %s',
                 self._options.bson_bind_address, self._options.json_bind_address)
        while True:
            try:
                msg = q.get()
                obj = bson.BSON(msg).decode()
                command = obj.pop('$command', None)
                command_handler = self.handlers.get(command, None)
                if command_handler is None:
                    log.warning('Unknown command %r', command)
                else:
                    command_handler(obj)
            except KeyboardInterrupt:
                raise
            except:
                log.exception('Error in message loop')

    def _handle_event(self, obj):
        '''Validate event, save to journal (and mongo)'''
        ev = model.event(obj).make(obj)
        self._j(ev)

    def _handle_event_noval(self, obj):
        '''Save to journal (and mongo)'''
        self._j(obj)

    def _g_aggregate(self):
        '''Execute all defined AggDef aggregations.'''
        zmr = self.context.socket(zmq.REQ)
        zmr.connect(self._options.zmr['req_uri'])
        sess =model.orm_session
        next_agg = datetime.utcnow()
        while True:
            try:
                now = datetime.utcnow()
                if now < next_agg:
                    gevent.sleep((next_agg-now).seconds)
                next_agg = datetime.utcnow() + timedelta(seconds=5)
                aggs = model.AggDef.query.find(dict(realtime=True)).all()
                if not aggs: continue
                begin = time.time()
                total_new = 0
                for ad in aggs:
                    new_docs = ad.incremental(zmr, self._options.zmr['event_limit'])
                    if new_docs and self._pub_sock:
                        self._pub_sock.send(bson.BSON.encode({
                                    '$command':'agg_complete',
                                    'name': ad.name,
                                    'docs':new_docs}))
                    total_new += new_docs
                    sess.flush()
                sess.close()
                if total_new:
                    log.info('%d aggregations complete in %.2fs', len(aggs),
                             time.time() - begin)
            except:
                log.exception('Error in aggregate')

def show_aggs():
    aggs = model.AggDef.query.find().all()
    for agg in aggs:
        log.info('Agg %s:', agg.name)
        for d in agg.collection.find():
            try:
                log.info('%s: %s', d['_id'].isoformat(' '), d['value'])
            except:
                log.info('%s', d)
    model.orm_session.close()
