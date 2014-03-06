'''Handle configuration for zarkov.

We support full configuration on the command line with defaults supplied by
either an .ini-style config file or a yaml (and thus json) config file.
'''
import sys
import logging.config
from optparse import OptionParser
from ConfigParser import ConfigParser

import yaml
import colander

import ming

log = logging.getLogger(__name__)

re_zmq = colander.Regex(
    r'(tcp|inproc)://(.+?)(:\d+)?',
    'Invalid zeromq URI')
re_ip_port = colander.Regex(
    r'(.+?)(:\d+)?',
    'Invalid address')
re_mongodb = colander.Regex(
    r'(mongodb|mim)://(.+?)(:\d+)?',
    'Invalid mongodb URI')

class BackdoorSchema(colander.MappingSchema):
    command = colander.SchemaNode(colander.String())
    port=colander.SchemaNode(colander.Int())

class BackdoorsSchema(colander.SequenceSchema):
    backdoor = BackdoorSchema()

class LogStreamPluginSchema(colander.MappingSchema):
    entry_point = colander.SchemaNode(colander.String())
    port = colander.SchemaNode(colander.Int())

class LogStreamSchema(colander.SequenceSchema):
    plugin = LogStreamPluginSchema()

class ZeroMQURIs(colander.SequenceSchema):
    uri = colander.SchemaNode(
        colander.String(), validator=re_zmq)

class LoadBalanceSchema(colander.MappingSchema):
    incoming_bind = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://0.0.0.0:6543')
    outgoing_uris = ZeroMQURIs()

class WebEventSchema(colander.MappingSchema):
    bind = colander.SchemaNode(
        colander.String(), validator=re_ip_port)

class DBSchema(colander.MappingSchema):
    name = colander.SchemaNode(colander.String())
    master = colander.SchemaNode(
        colander.String(), validator=re_mongodb,
        missing='mongodb://localhost:27017')
    database = colander.SchemaNode(colander.String())
    use_gevent = colander.SchemaNode(colander.Bool(), missing=True)

class ExtraDBSchema(colander.SequenceSchema):
    dbs = DBSchema()

class ZMRConfigSchema(colander.MappingSchema):
    req_uri = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://127.0.0.1:5555')
    req_bind = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://0.0.0.0:5555')
    worker_uri = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://0.0.0.0')
    reduce_count = colander.SchemaNode(
        colander.Int(), missing=256)
    event_limit = colander.SchemaNode(
        colander.Int(), missing=100000)
    job_root = colander.SchemaNode(
        colander.String(), missing='/tmp/zmr')
    map_chunk_size = colander.SchemaNode(
        colander.Int(), missing=5e6)
    map_chunks_per_page = colander.SchemaNode(
        colander.Int(), missing=20)
    outstanding_chunks = colander.SchemaNode(
        colander.Int(), missing=4)
    max_chunk_timeout = colander.SchemaNode(
        colander.Int(), missing=600)
    request_greenlets = colander.SchemaNode(
        colander.Int(), missing=16)
    compress = colander.SchemaNode(
        colander.Int(), missing=0)
    src_port = colander.SchemaNode(
        colander.Int(), missing=0)
    sink_port = colander.SchemaNode(
        colander.Int(), missing=0)
    processes_per_worker = colander.SchemaNode(
        colander.Int(), missing=0)
    requests_per_worker_process = colander.SchemaNode(
        colander.Int(), missing=256)
    suicide_level = colander.SchemaNode(
        colander.Int(), missing=3 * 2**20)

class ConfigSchema(colander.MappingSchema):
    bson_uri = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://127.0.0.1:6543')
    json_uri = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://1227.0.0.1:6544')
    bson_bind_address = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://0.0.0.0:6543')
    json_bind_address = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://0.0.0.0:6544')
    publish_bind_address = colander.SchemaNode(
        colander.String(), validator=re_zmq,
        missing='tcp://0.0.0.0:6545')
    web_port = colander.SchemaNode(colander.Int(), missing=8081)
    backdoor = BackdoorsSchema(missing=[])
    mongo_uri = colander.SchemaNode(
        colander.String(), validator=re_mongodb,
        missing='mongodb://localhost:27017')
    mongo_database = colander.SchemaNode(
        colander.String(), missing='zarkov')
    mongo_username = colander.SchemaNode(
        colander.String(), missing=None)
    mongo_password = colander.SchemaNode(
        colander.String(), missing=None)
    verbose = colander.SchemaNode(
        colander.Bool(), missing=False)
    incremental = colander.SchemaNode(
        colander.Bool(), missing=True)
    num_event_servers = colander.SchemaNode(
        colander.Int(), missing=0)
    num_event_logs = colander.SchemaNode(
        colander.Int(), missing=4)
    journal = colander.SchemaNode(
        colander.String(), missing='journal')
    journal_file_size = colander.SchemaNode(
        colander.Int(), missing=2**18)
    journal_min_files = colander.SchemaNode(
        colander.Int(), missing=4)
    zmr = ZMRConfigSchema(missing=None)
    logstream = LogStreamSchema(missing=None)
    loadbalance = LoadBalanceSchema(missing=None)
    webevent = WebEventSchema(missing=None)
    extra_dbs = ExtraDBSchema(missing=[])
    extra = colander.SchemaNode(colander.Mapping(), missing={})
    flush_mmap = colander.SchemaNode(
            colander.Bool(), missing=False)

def configure(args=None):
    '''Load the options and configure the system'''
    if args is None: args = sys.argv
    options, args = get_options(args)
    if options.verbose:
        log.info('Settings:')
        for k,v in sorted(options.__dict__.items()):
            log.info('  %s: %r', k, v)
    ming_config = {
        'ming.zarkov.master':options.mongo_uri,
        'ming.zarkov.database':options.mongo_database,
        'ming.zarkov.use_gevent':True}
    for dbinfo in options.extra_dbs:
        dbinfo = dict(dbinfo)
        prefix = 'ming.%s.' % dbinfo.pop('name')
        for k,v in dbinfo.items():
            ming_config[prefix + k] = v
    if options.mongo_username:
        ming_config['ming.zarkov.authenticate.name'] = options.mongo_username
    if options.mongo_username:
        ming_config['ming.zarkov.authenticate.password'] = options.mongo_password
    ming.configure(**ming_config)
    if options.pdb:
        sys.excepthook = postmortem_hook
    return options, args

def get_options(argv):
    '''Load the options from argv and any config files specified'''
    defaults=dict(
        bind_address='tcp://0.0.0.0:6543',
        backdoor=None,
        password=None,
        mongo_uri='mongodb://127.0.0.1:27017',
        mongo_database='zarkov',
        journal='journal',
        verbose=False,
        incremental=10)
    optparser = get_parser(defaults)
    options, args = optparser.parse_args(argv)
    config_schema = ConfigSchema()
    defaults.update(config_schema.deserialize({}))
    if options.ini_file:
        config = ConfigParser()
        config.read(options.ini_file)
        log.info('About to configure logging')
        logging.config.fileConfig(options.ini_file, disable_existing_loggers=False)
        log.info('Configured logging')
        if config.has_section('zarkov'):
            defaults.update(
                (k, eval(v)) for k,v in config.items('zarkov'))
    if options.yaml_file:
        with open(options.yaml_file) as fp:
            yaml_obj = yaml.load(fp.read())
            yaml_obj = config_schema.deserialize(yaml_obj)
            if yaml_obj:
                defaults.update(yaml_obj)
            else:
                log.warning('No configuration found -- empty yaml file %r?',
                            options.yaml_file)
    optparser = get_parser(defaults)
    options, args = optparser.parse_args(argv)
    return options, args

def get_parser(defaults):
    '''Build a command line OptionParser based on the given defaults'''
    optparser = OptionParser(
        usage=('%prog [--options]')) 
    optparser.set_defaults(**defaults)

    optparser.add_option(
        '-i', '--config-ini', dest='ini_file',
        help='Load options from config (.ini) file')
    optparser.add_option(
        '-y', '--config-yaml', dest='yaml_file',
        help='Load options from config (.yaml) file')
    optparser.add_option(
        '-l', '--listen', dest='bind_address',
        help='IP address on which to listen for connections')
    optparser.add_option(
        '-p', '--port', dest='port',
        type='int',
        help='Port to listen for connections')
    optparser.add_option(
        '--password', dest='password',
        help='Password to require for connection')
    optparser.add_option(
        '--mongo-uri', dest='mongo_uri',
        help='URI for MongoDB server in which to store data')
    optparser.add_option(
        '--mongo-database', dest='mongo_database',
        help='MongoDB database in which to store data')
    optparser.add_option(
        '--journal', dest='journal',
        help='Filename to use for journalling')
    optparser.add_option(
        '--pdb', dest='pdb', action='store_true',
        help='Drop into pdb on unhandled exceptions')
    optparser.add_option(
        '--profile', dest='profile', 
        help='Profile the run into the given filename')
    optparser.add_option(
        '-v', '--verbose', dest='verbose',
        action='store_true')
    optparser.add_option(
        '-b', '--backdoor', dest='backdoor')
    optparser.add_option(
        '--incremental', dest='incremental',
        type='int',
        help=('how many events to log before triggering an incremental aggregation'
              ' (0 to disable)'))
    return optparser

def postmortem_hook(etype, value, tb): # pragma no cover
    import pdb, traceback
    try:
        from IPython.core.debugger import Pdb
        sys.stderr.write('Entering post-mortem IPDB shell\n')
        p = Pdb(color_scheme='Linux')
        p.reset()
        p.setup(None, tb)
        p.print_stack_trace()
        sys.stderr.write('%s: %s\n' % ( etype, value))
        p.cmdloop()
        p.forget()
        # p.interaction(None, tb)
    except ImportError:
        sys.stderr.write('Entering post-mortem PDB shell\n')
        traceback.print_exception(etype, value, tb)
        pdb.post_mortem(tb)

