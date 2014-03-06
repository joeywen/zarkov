import uuid
import logging
import hashlib

import bson
import pkg_resources
from gevent.pool import Pool
from gevent.pywsgi import WSGIServer
from gevent_zeromq import zmq
from webob import Request, Response

from zarkov import util
from .base import Command

log = logging.getLogger(__name__)

HWM=1000

class WebEventCommand(Command):
    name='web-event'
    help=("Run the Zarkov web event server")
    min_args = max_args = 0

    def run(self):
        context = zmq.Context()
        zsock = context.socket(zmq.PUSH)
        zsock.connect(self.options.bson_uri)
        hhost, hport = self.options.webevent['bind'].split(':')
        web_server = WSGIServer(
            (hhost, int(hport)),
            WebEventApp(zsock),
            log=util.LogFile(__name__, logging.DEBUG))
        web_server.serve_forever()

class WebEventApp(object):
    cookie_name = 'zess'
    cookie_max_age = 60*60*24*365*10
    greenlet_pool_size=10

    def __init__(self, sock):
        self._sock = sock
        fn = pkg_resources.resource_filename('zarkov', 'spacer.gif')
        self._gif = open(fn, 'rb').read()
        self._pool = Pool(self.greenlet_pool_size)
    
    def __call__(self, environ, start_response):
        req = Request(environ)
        res = Response(cache_control='no-cache', pragma='no-cache')
        session = req.cookies.get(self.cookie_name, None)
        if session is None:
            session = gen_session()
            res.set_cookie(
                self.cookie_name, session,
                max_age=self.cookie_max_age,
                path='/')
        self.send_event(session, req)
        res.content_type = 'image/gif'
        del res.charset
        res.body = self._gif
        return res(environ, start_response)

    def send_event(self, session, req):
        ev = self._event_from_request(session, req)
        self._sock.send(bson.BSON.encode(ev))

    def _event_from_request(self, session, req):
        return dict(
            type='web',
            context={
                self.cookie_name:session,
                'user_agent':req.user_agent,
                'remote_addr':req.headers.get('X_FORWARDED_FOR', req.remote_addr),
                'referer':req.referer,
                'script_name':req.script_name,
                'path_info':req.path_info,
                'query_string':req.query_string,
                },
            extra=req.body,
            )
        
def gen_session():
    return str(uuid.uuid1())
    return hashlib.sha1(open('/dev/urandom').read(40)).hexdigest()
