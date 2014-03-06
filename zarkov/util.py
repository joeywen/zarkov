import json
import struct
import logging
import inspect
import textwrap
from datetime import datetime

import bson
import gevent
from gevent_zeromq import zmq

log = logging.getLogger(__name__)

def bson_from_json(msg):
    def object_hook(dct):
        if '$datetime' in dct:
            return datetime.utcfromtimestamp(dct['$datetime'])
        return dct
    obj = json.loads(msg, object_hook=object_hook)
    return bson.BSON.encode(obj)

def func_text(func):
    '''Return the text of a python function'''
    if isinstance(func, basestring):
        src = func
    else:
        src = inspect.getsource(func)
    src = textwrap.dedent(src)
    return src

def send_bson(sock, obj, *args):
    '''Send a bson object over a zeromq socket'''
    msg = bson.BSON.encode(obj)
    sock.send(msg, *args)

def recv_bson(sock):
    '''Receive a bson object over a zeromq socket'''
    msg = sock.recv()
    obj = bson.BSON(msg).decode()
    return obj

def req_bson(sock, obj):
    '''Send and then receive a bson object over a zeromq socket'''
    send_bson(sock, obj)
    return recv_bson(sock)

def bson_iter(data, as_class=dict, tz_aware=False):
    '''Yield successive bson docs from a string containing zero or more docs'''
    def raw_iter(data):
        pos = 0
        ldata = len(data)
        while pos < ldata:
            sz = struct.unpack('<i', data[pos:pos+4])[0]
            yield bson.BSON(data[pos:pos+sz])
            pos += sz
    for bobj in raw_iter(data):
        yield bobj.decode(as_class, tz_aware)

def bson_file_iter(fp, as_class=dict, tz_aware=False, decode=True):
    '''Yield successive bson docs from an open file'''
    while True:
        data = fp.read(4)
        if not data: break
        sz = struct.unpack('<i', data)[0]
        data += fp.read(sz-4)
        if decode:
            yield bson.BSON(data).decode(as_class, tz_aware)
        else:
            yield data

def gevent_queue(frontend, backend):
    '''Gevent-ready device'''
    gevent.spawn(zeromq_relay, frontend, backend)
    gevent.spawn(zeromq_relay, backend, frontend)

def gevent_pipe(frontend, backend):
    '''Gevent-ready device'''
    gevent.spawn(zeromq_relay, frontend, backend)

def zeromq_relay(a, b):
    '''Copy data from zeromq socket a to zeromq socket b'''
    while True:
        msg = a.recv()
        more = a.getsockopt(zmq.RCVMORE)
        if more:
            b.send(msg, zmq.SNDMORE)
        else:
            b.send(msg)

class LogFile(object):
    def __init__(self, name, level):
        self._logger = logging.getLogger(name)
        self._level = level
    def write(self, text):
        self._logger.log(self._level, text.strip())

def item_chunked_iter(it, chunksize):
    '''Yields chunks of at most 'chunksize' items from the iterator'''
    chunk = []
    for o in it:
        chunk.append(o)
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []
    if chunk: yield chunk

class ending_request(object):

    def __init__(self, mongodb):
        self.mongodb = mongodb

    def __enter__(self):
        pass

    def __exit__(self, e_type, e_value, e_tb):
        self.mongodb.end_request()
        if e_type:
            raise e_type, e_value, e_tb

    def __call__(self, func, *args, **kwargs):
        with self:
            return func(*args, **kwargs)

def dotted_lookup(d, key):
    for k in key.split('.'):
        d = d[k]
    return d
