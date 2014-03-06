import string
import logging
import urlparse

from setproctitle import setproctitle, getproctitle

from zarkov import exceptions as exc

class Command(object):
    _registry = {}
    name=None
    min_args=0
    max_args=0
    help='-- no help --'
    detailed_help=''

    class __metaclass__(type):
        def __init__(cls, name, bases, dct):
            name = getattr(cls, 'name', None)
            if name:
                cls._registry[name] = cls

    def __init__(self, options, *args):
        log = logging.getLogger(__name__)
        setproctitle('zcmd:%s [%s]' % (self.name, getproctitle()))
        self.options = options
        for bd in self.options.backdoor:
            if bd['command'] == self.name:
                from gevent import backdoor
                try:
                    s = backdoor.BackdoorServer(('127.0.0.1', bd['port']))
                    s.start()
                    log.info('Started backdoor server on 127.0.0.1:%s', bd['port'])
                except:
                    log.exception('Failed to start backdoor server on 127.0.0.1:%s', bd['port'])
        self.args = args
        if len(args) < self.min_args:
            raise exc.CommandError(
                'Need at least %d arguments' % self.min_args)
        if self.max_args is not None and len(args) > self.max_args:
            raise exc.CommandError(
                   'Need no more than %d arguments' % self.max_args)

    @classmethod
    def lookup(cls, name):
        try:
            return cls._registry[name]
        except KeyError:
            raise exc.CommandError(
                '%s not found; maybe you meant one of %r?' % (
                    name, cls._registry.keys()))

    def run(self):
        raise NotImplementedError, 'run'

    def subcommand(self, name, *args):
        Cmd = self.lookup(name)
        cmd = Cmd(self.options, *args)
        return cmd.run()

    def mongorestore_command(self, bsonfile, collection,
                     host=None, port=None, database=None):
        '''Command to the given bsonfile into the given collection'''
        mongo_parsed = urlparse.urlparse(self.options.mongo_uri)
        parsed_host, parsed_port = mongo_parsed.netloc.split(':')
        parsed_database = self.options.mongo_database
        if host is None: host = parsed_host
        if port is None: port = parsed_port
        if database is None: database = parsed_database
        restore_tpl=string.Template(
            'mongorestore -h $host --port $port -d $database -c $collection $bsonfile')
        return restore_tpl.substitute(dict(
                host=host, port=port, database=database,
                collection=collection, bsonfile=bsonfile))
            
