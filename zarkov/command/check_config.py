import logging

from gevent.pywsgi import WSGIServer

from zarkov import web, util
from .base import Command

log = logging.getLogger(__name__)

class CheckConfigCommand(Command):
    name='check-config'
    help=("Check the config file for compliance (basically a no-op)")
    
    min_args=max_args=0

    def run(self):
        log.info('No config problems found')

