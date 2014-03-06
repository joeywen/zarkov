import os
import shutil
import mmap
import logging
import resource
import tempfile
import traceback
from glob import glob
from pprint import pformat
from itertools import groupby
from functools import partial
from datetime import datetime

import bson
import gevent
from gevent.queue import Queue
from zarkov import util
from zarkov.model import IdGen
from zarkov import exceptions as zexc

log = logging.getLogger(__name__)

class Job(object):

    def __init__(self, router, command):
        self.router = router
        self.command = command
        self.id = IdGen.get_ids()[0]
        self.status = dict(state='ready')
        self._created = datetime.utcnow()

    @classmethod
    def from_request(cls, router, req):
        cls = dict(
            mapreduce=MRJob,
            xmapreduce=MRJob,
            basic=BasicJob)[req['$command']]
        return cls.from_request(router, req)

class BasicJob(Job):

    def __init__(self, router, command, func_text, func_name,
                 args, kwargs):
        super(BasicJob, self).__init__(router, command)
        self.func_text = func_text
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs
        self.kwargs['job_id'] = self.id

    @classmethod
    def from_request(cls, router, req):
        command = req.pop('$command')
        return cls(
            router,
            command=command,
            func_text=req['func_text'],
            func_name=req['func_name'],
            args=req['args'],
            kwargs=req['kwargs'])

    def run(self):
        try:
            self.status['state'] = 'running'
            hdr = dict(
                jobtype='basic',
                func_text=self.func_text,
                func_name=self.func_name,
                args=self.args,
                kwargs=self.kwargs)
            self.status['state'] = 'running'
            for header, content in self.router.job_manager(
                self, 'basic', [ (hdr,) ] ):
                pass
            self.status['state'] = 'complete'
        except zexc.WorkerError, err:
            self.retire('error', err.args[0])
            return
        except:
            self.retire('error', traceback.format_exc())
            return

    def retire(self, state, traceback=None):
        log.debug('%s: retire', self.id)
        self.status['state'] = state
        self.status['traceback'] = traceback
        if state == 'error':
            log.error('Traceback:\n%s', traceback)
        log.info('%s: BasicJob: Complete %s',
                 datetime.utcnow() - self._created,
                 self.id)

class MRJob(Job):
    MAP_IN_TPL='map_in_%s.bson'
    FINALIZE_IN_FN='finalize_in.bson'
    MAP_OUTPUT_TPL='map_out_%s.bson'

    def __init__(self, router, command,
                 database, input_names, query,
                 map_text, reduce_text, finalize_text,
                 map_name, reduce_name, finalize_name,
                 output_name, output_type):
        '''out_type is one of (replace, merge, reduce), same as mongodb
        '''
        super(MRJob, self).__init__(router, command)
        # Copy params
        self._database = database
        self._input_names = input_names
        self._query = query
        self._map_text = map_text
        self._reduce_text = reduce_text
        self._finalize_text = finalize_text
        self._map_name = map_name
        self._reduce_name = reduce_name
        self._finalize_name = finalize_name
        self._output_name = output_name
        self._output_type = output_type

        # Save some router params locally
        self.options = router.options
        self._mongodb = router.mongodb

        # Set up scoreboard
        self.phases = []
        job_root = self.router.options.zmr['job_root']
        if not os.path.exists(job_root):
            os.makedirs(job_root)
        self.jobdir = tempfile.mkdtemp(
            prefix=output_name + '.',
            dir=job_root)
        self._mmaps = []
        log.debug('=== Create job for %s to %s === ', self._output_type, self._output_name)
        if self._query:
            log.debug('Query:\n%s', pformat(self._query))
        if self._map_text:
            log.debug('Map(%s):\n%s', self._map_name, self._map_text)
        if self._reduce_text:
            log.debug('Reduce(%s):\n%s', self._reduce_name, self._reduce_text)

    def retire(self, state, traceback=None):
        log.debug('%s: retire', self.id)
        self.status['state'] = state
        self.status['traceback'] = traceback
        for m in self._mmaps:
            m.close()
        if state == 'error':
            log.error('Traceback:\n%s', traceback)
        shutil.rmtree(self.jobdir)
        log.info('%s: %ss: Complete %s',
                 self._output_name,
                 datetime.utcnow() - self._created,
                 self.id)

    def map_read(self, fn):
        fp = open(fn, 'rb')
        try:
            mm = mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ)
        except mmap.error:
            log.exception('Error mapping %s for read, possibly empty', fn)
            return ''
        self._mmaps.append(mm)
        return mm

    @classmethod
    def from_request(cls, router, req):
        command = req.pop('$command')
        query = req.get('query', None)
        if query is None: query = {}
        else: query = bson.BSON(query).decode()
        return cls(
            router,
            command=command,
            database=req.get('database', 'zarkov'),
            input_names=req.get('input_names', ['event', 'event.0']),
            query=query,
            map_text=req.get('map_text', None),
            reduce_text=req.get('reduce_text', None),
            finalize_text=req.get('finalize_text', None),
            map_name=req.get('map_name', None),
            reduce_name=req.get('reduce_name', None),
            finalize_name=req.get('finalize_name', None),
            output_name=req.get('output_name', 'zagg.result'),
            output_type=req.get('output_type', 'replace'))

    @property
    def input_collections(self):
        db = self.db
        return [ db[i] for i in self._input_names ]

    @property
    def db(self):
        return self._mongodb[self._database]

    @property
    def output_collection(self):
        return self._mongodb[self._database][self._output_name]

    @property
    def query(self):
        q = self._query or {}
        for ic in self.input_collections:
            for x in ic.find(q):
                yield x

    def reducefun(self, *args, **kwargs):
        if not self._reduce_text: return None
        ns = {}
        exec self._reduce_text in ns
        fun = ns[self._reduce_name]
        if args or kwargs:
            return partial(fun, *args, **kwargs)
        else:
            return fun

    def run(self):
        try:
            # Start the query/map phase
            log.debug('Starting queries')
            gquery = MRQuery(self, self.query)
            gmapper = MRMapper(self, self._map_text, self._map_name)
            pipeline = (gquery >> gmapper)
            log.info('%s: %s: %r', self.id, self._output_name, pipeline)
            pipeline.start(self)

            # Join the query and mapper
            self.status['state'] = 'query/map'
            gquery.join()
            self.status['state'] = 'map'
            gmapper.join()

            # Start the sort/reduce/commit phase
            greducer = MRReducer(self, self._reduce_text, self._reduce_name)
            gfinalizer = MRFinalizer(self, self._finalize_text, self._finalize_name)
            if self.command == 'mapreduce':
                gcommitter = MRCommitter(
                    self, self._output_type, self._output_name,
                    self.reducefun())
            elif self.command == 'xmapreduce':
                gcommitter = XMRCommitter(self, self._output_type)
            pipeline = (greducer >> gfinalizer >> gcommitter)
            log.info('%s: %s: %r', self.id, self._output_name, pipeline)
            pipeline.start(self)

            # Join the reduce/finalize/commit
            self.status['state'] = 'reduce/finalize/commit'
            greducer.join()
            self.status['state'] = 'finalize/commit'
            gfinalizer.join()
            self.status['state'] = 'commit'
            gcommitter.join()
        except zexc.WorkerError, err:
            self.retire('error', err.args[0])
            return
        except:
            self.retire('error', traceback.format_exc())
            return
        self.retire('complete')

class JobPhase(object):
    _greenlet = None

    def join(self, *args, **kwargs):
        self._greenlet.get(*args, **kwargs)

    def __repr__(self):
        status = ''
        if self._greenlet:
            status = 'waiting'
            if self._greenlet.started:
                status = 'running'
            if self._greenlet.successful():
                status = 'complete'
            elif self._greenlet.dead:
                status = 'dead'
        return '<%s %s>' % (
            self.__class__.__name__, status)

    def __rshift__(self, other):
        return Pipeline([self, other])

class Pipeline(JobPhase):

    def __init__(self, parts):
        self.parts = parts

    def start(self, job):
        q = None
        for p in self.parts:
            p.input = q
            p.output = q = Queue(1000)
        for p in self.parts:
            p._greenlet = gevent.spawn_link_exception(util.ending_request(job._mongodb), p.run)

    def __rshift__(self, other):
        return Pipeline(self.parts + [ other ])

    def __repr__(self):
        return ' ==> '.join(map(repr, self.parts))

class MRQuery(JobPhase):
    '''Generate the input for a mapreduce job -- NOT distributed'''

    def __init__(self, job, query):
        self._job = job
        self._job.phases.append(self)
        self._query = query
        self._chunk_size = job.options.zmr['map_chunk_size']
        self._chunks_per_page = job.options.zmr['map_chunks_per_page']
        self._fn_tpl = os.path.join(
            self._job.jobdir, self._job.MAP_IN_TPL)
        self._job.status['query'] = 'Startup'

    def run(self):
        it = ( bson.BSON.encode(obj) for obj in self._query )
        chunks = size_chunked_iter(it, self._chunk_size)
        pagenum = None
        for pagenum, page_chunks in enumerate(
            util.item_chunked_iter(chunks, self._chunks_per_page)):
            self._job.status['query'] = 'Processed %d records' % (pagenum * self._chunks_per_page)
            fn = self._fn_tpl % pagenum
            log.debug('fn = %s', fn)
            index = []
            with open(fn, 'wb') as fp:
                pos = 0
                for chunk in page_chunks:
                    old = pos
                    fp.write(chunk)
                    pos = fp.tell()
                    index.append((old, pos))
            mm = self._job.map_read(fn)
            for b,e in index:
                log.debug('query put(%s,%s)', b,e)
                self.output.put((mm, b, e))
        if pagenum is None:
            log.info('Nothing to process from this query.  Job %s' % self._job.id)
        self.output.put(StopIteration)
        del self._job.status['query']

class MRMapper(JobPhase):
    '''Generate the input for a mapreduce job -- distributed to all workers'''

    def __init__(self, job, map_text, map_name):
        self._job = job
        self._job.phases.append(self)
        self._map_text = map_text
        self._map_name = map_name
        self._job.status['map'] = 'Startup'

    def run(self):
        if self._job.command == 'mapreduce':
            jobtype = 'map'
        elif self._job.command == 'xmapreduce':
            jobtype = 'xmap'
        hdr = dict(
            jobtype=jobtype,
            map_text=self._map_text,
            map_name=self._map_name,
            reduce_count=self._job.options.zmr['reduce_count'],
            compress=self._job.options.zmr['compress'])
        chunk_iter = ((hdr, partial(mmap_reader, mm, b, e)) for mm, b, e in self.input)
        result_iter = self._job.router.job_manager(
            self._job, jobtype, chunk_iter)
        for i, (header, parts) in enumerate(result_iter):
            self._job.status['map'] = 'Processed %d chunks' % i
            for j, part in enumerate(parts):
                if not part: continue
                fn = os.path.join(
                    self._job.jobdir,
                    self._job.MAP_OUTPUT_TPL % j)
                with open(fn, 'ab') as fp:
                    r = resource.getrusage(resource.RUSAGE_SELF)
                    log.debug('Retire map %s, rss %s:%s', j, i, r.ru_maxrss)
                    fp.write(part)
        del self._job.status['map']

class MRReducer(JobPhase):
    '''Generate the input for a mapreduce job'''

    def __init__(self, job, reduce_text, reduce_name):
        self._job = job
        self._job.phases.append(self)
        self._reduce_text = reduce_text
        self._reduce_name = reduce_name
        self._job.status['reduce'] = 'Startup'

    def run(self):
        # Short circuit
        if not self._reduce_text:
            pattern = os.path.join(
                self._job.jobdir, self._job.MAP_OUTPUT_TPL % '*')
            for fn in glob(pattern):
                if not os.path.exists(fn): continue
                mm = self._job.map_read(fn)
                for obj in util.bson_iter(mm):
                    self.output.put(obj)
            self.output.put(StopIteration)
            return
        # Divvy out jobs to workers
        def chunk_gen(hdr):
            for j in xrange(self._job.options.zmr['reduce_count']):
                fn = os.path.join(
                    self._job.jobdir, self._job.MAP_OUTPUT_TPL % j)
                if not os.path.exists(fn): continue
                yield dict(hdr), lambda: [ self._job.map_read(fn) ]
        if self._job.command == 'mapreduce':
            jobtype = 'reduce'
        elif self._job.command == 'xmapreduce':
            jobtype = 'xreduce'
        hdr = dict(
            jobtype=jobtype,
            reduce_text=self._reduce_text,
            reduce_name=self._reduce_name,
            compress=self._job.options.zmr['compress'])
        result_iter = self._job.router.job_manager(
            self._job, jobtype, chunk_gen(hdr))
        for i, (header, content) in enumerate(result_iter):
            self._job.status['reduce'] = 'Processed %d chunks' % i
            r = resource.getrusage(resource.RUSAGE_SELF)
            log.debug('Retire reduce, rss %s', r.ru_maxrss)
            for part in content:
                for d in util.bson_iter(part):
                    for obj in d['result']:
                        self.output.put(obj)
        self.output.put(StopIteration)

class MRFinalizer(JobPhase):
    '''Generate the input for a mapreduce job'''

    def __init__(self, job, finalize_text, finalize_name):
        self._job = job
        self._job.phases.append(self)
        self._finalize_text = finalize_text
        self._finalize_name = finalize_name
        self._fn = os.path.join(
            self._job.jobdir, self._job.FINALIZE_IN_FN)
        self._job.status['finalize'] = 'Startup'

    def run(self):
        # Short circuit
        if not self._finalize_text:
            for obj in self.input:
                self.output.put(obj)
            self.output.put(StopIteration)
            return

        with open(self._fn, 'wb') as fp:
            for obj in self.input:
                fp.write(bson.BSON.encode(obj))
        mm = self._job.map_read(self._fn)
        if not mm:
            self.output.put(StopIteration)
            return
        hdr = dict(
            jobtype='finalize',
            finalize_text=self._finalize_text,
            finalize_name=self._finalize_name,
            compress=self._job.options.zmr['compress'])

        def chunk_gen():
            yield dict(hdr), lambda:mm
        result_iter = self._job.router.job_manager(
            self._job, 'finalize', chunk_gen())

        for i, (header, content) in enumerate(result_iter):
            self._job.status['finalize'] = 'Processed %d chunks' % i
            r = resource.getrusage(resource.RUSAGE_SELF)
            log.debug('Retire finalize, rss %s', r.ru_maxrss)
            for part in content:
                for obj in util.bson_iter(part):
                    self.output.put(obj)
        self.output.put(StopIteration)
        del self._job.status['finalize']

class MRCommitter(JobPhase):

    def __init__(self, job, output_type, cname, reducefun):
        self._job = job
        self._job.phases.append(self)
        self._cname = cname
        self._reducefun = reducefun
        func = dict(
            replace=self.run_replace,
            merge=self.run_merge,
            reduce=self.run_reduce)[output_type]
        self.run = func

    @property
    def _iter(self):
        # Seems to be required due to some weirdness in gevent queues
        def anno_job():
            for o in self.input:
                yield dict(o, job_id=self._job.id)
        result = anno_job()
        result = util.item_chunked_iter(result, 100)
        return result

    def run_replace(self):
        ocoll = self._job.db[self._cname]
        ocoll.drop()
        num_inserts = 0
        for objs in self._iter:
            num_inserts += len(objs)
            ocoll.insert(objs, safe=False)

    def run_merge(self):
        ocoll = self._job.db[self._cname]
        for objs in self._iter:
            for o in objs:
                ocoll.save(o, safe=False)

    def run_reduce(self):
        ocoll = self._job.db[self._cname]
        for objs in self._iter:
            keys = [ o['_id'] for o in objs ]
            existing = dict(
                (o['_id'], o['value'])
                for o in ocoll.find(dict(_id={'$in':keys})))
            to_insert = []
            for o in objs:
                k,new_v = o['_id'], o['value']
                old_v = existing.get(k)
                if old_v is None or self._reducefun is None:
                    to_insert.append(o)
                else:
                    o['value'] = self._reducefun(k, [ old_v, new_v ])
                    ocoll.save(o, safe=False)
            if to_insert: ocoll.insert(to_insert, safe=False)

class XMRCommitter(JobPhase):

    def __init__(self, job, output_type):
        self._job = job
        self._job.phases.append(self)
        self._output_type = output_type

    @property
    def iter(self):
        def split_coll(q):
            for doc in q:
                cname = doc.pop('c')
                yield cname, dict(_id=doc['k'], value=doc['v'])
        return groupby(split_coll(self.input), key=lambda (c,d):c)

    def run(self):
        for cname, values in self.iter:
            ci = MRCommitter(
                self._job, self._output_type, cname,
                self._job.reducefun(cname))
            ci.input = Queue(1000)
            ci._greenlet = gevent.spawn_link_exception(util.ending_request(self._job._mongodb), ci.run_wrapper)
            for v in values:
                ci.input.put(v[1])
            ci.input.put(StopIteration)
            ci._greenlet.join()

def size_chunked_iter(it, chunksize):
    '''Yields strings of at most 'chunksize' bytes from the string iterator'''
    chunk = []
    bytes = 0
    for o in it:
        l = len(o)
        if bytes + l > chunksize:
            yield ''.join(chunk)
            bytes = 0
            chunk = []
        chunk.append(o)
        bytes += l
    if chunk:
        yield ''.join(chunk)

def mmap_reader(mmap, begin, end):
    value = mmap[begin:end]
    assert len(value) == end-begin, 'mmap strageness'
    yield value
