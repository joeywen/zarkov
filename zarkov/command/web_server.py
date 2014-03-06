import logging

from gevent.pywsgi import WSGIServer

from zarkov import web, util
from .base import Command

log = logging.getLogger(__name__)

class WebServerCommand(Command):
    name='web-server'
    help=("Run the Zarkov web server")
    
    min_args=max_args=0

    def run(self):
        web_server = WSGIServer(
            ('', self.options.web_port),
            web.WebApp(),
            log=util.LogFile('zarkov.wsgi', logging.DEBUG))
        web_server.serve_forever()

