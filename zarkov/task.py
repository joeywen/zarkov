import time
import logging
from datetime import datetime

import bson

from zarkov.util import req_bson, func_text
from zarkov import model as ZM
from zarkov import exceptions as zexc

log = logging.getLogger(__name__)

class Task(object):
    '''Zarkov aggregation task. Operates on a set of Zarkov events and
    incrementally updates one or more aggregate collections
    '''

    def __init__(
        self, aggdef, limit=None, inputs=None, id_key=None,
        id_min=None, id_max=None, is_subtask=False):
        self._def = aggdef
        self._query = {}
        self._limit = limit
        self._status = None
        self._job_ids = []
        self._results = []
        self._id_key = id_key or self._def.id_key
        if inputs is None:
            inputs = self._def.inputs
        self._inputs = inputs
        if id_min is None:
            self.id_min = self._def.last_id
        else:
            self.id_min = id_min
        if id_max is None:
            self.id_max = self._calculate_id_max()
        else:
            self.id_max = id_max
        self.is_subtask = is_subtask
        self._query = {
            self._id_key: {'$gt':  self.id_min, '$lte': self.id_max } }

    def start(self, socket, full=False):
        # Grab all the events created since I was last run
        if not self.is_subtask:
            self._def.last_id = self.id_max
            self._def.last_id_timestamp = datetime.utcnow()
        self._start_jobs(socket, full)
        return self.id_max - self.id_min

    def subtask(self, cls, *args, **kwargs):
        new_kwargs = dict(
            is_subtask=True,
            id_min=self.id_min,
            id_max=self.id_max)
        new_kwargs.update(kwargs)
        return cls(*args, **new_kwargs)

    def join(self, socket):
        while self._job_ids:
            r = req_bson(socket, { '$command': 'status', 'job_id':self._job_ids[-1]})
            log.info('Wait: %r', r)
            state = r['status']['state']
            if state == 'error':
                log.error('Traceback:\n%s', r['status']['traceback'])
                raise zexc.WorkerError(r['status']['traceback'])
            elif state == 'UNKNOWN':
                log.error('Unknown status, terminating (maybe you restarted the zmr-router?)')
                raise zexc.WorkerError('Unknown')
            elif state == 'complete':
                self._results.append(r)
                self._job_ids.pop()
            else:
                time.sleep(1)
        return self._results

    def run(self, socket, full=False):
        results = []
        count = self.start(socket, full)
        results.append(self.join(socket))
        return count

    def start_basic_job(self, socket, func, *args, **kwargs):
        '''Start a (single-threaded) job to be run on a worker.'''
        req = {
            '$command': 'basic',
            'database':'zarkov',
            'args':list(args),
            'kwargs':kwargs }
        req['func_name'] = func.func_name
        req['func_text'] = func_text(func)
        r = req_bson(socket, req)
        self._job_ids.append(r['job_id'])
        return r

    def start_mr(self, socket, full, collection, filter=None,
                 map=None, reduce=None, finalize=None,
                 map_name=None, reduce_name=None, finalize_name=None,
                 inputs=None, output_type=None):
        if inputs is None:
            inputs = self._inputs
        req = {
            '$command': 'mapreduce',
            'database':'zarkov',
            'input_names':inputs,
            'output_name':collection.name}
        if full:
            req['output_type'] = 'replace'
        else:
            req['output_type'] = 'reduce'
        if output_type is not None:
            req['output_type'] = output_type
        query = dict(self._query)
        if filter: query.update(filter)
        log.debug('query=%s' % query)
        req['query'] = bson.Binary(bson.BSON.encode(query))
        if map is not None:
            if map_name is None: map_name = map.func_name
            req.update(map_text=func_text(map), map_name=map_name)
        if reduce is not None:
            if reduce_name is None: reduce_name = reduce.func_name
            req.update(reduce_text=func_text(reduce), reduce_name=reduce_name)
        if finalize is not None:
            if finalize_name is None: finalize_name = finalize.func_name
            req.update(finalize_text=func_text(finalize), finalize_name=finalize_name)
        r = req_bson(socket, req)
        self._job_ids.append(r['job_id'])
        return r

    def start_xmr(self, socket, full, filter=None,
                  map=None, reduce=None, finalize=None,
                  map_name=None, reduce_name=None, finalize_name=None,
                  input_name='event', name=None):
        req = {
            '$command': 'xmapreduce',
            'output_name':name or 'zagg.result',
            'database':'zarkov',
            'input_name':input_name }
        if full:
            req['output_type'] = 'replace'
        else:
            req['output_type'] = 'reduce'
        query = dict(self._query)
        if filter: query.update(filter)
        log.debug('query=%s' % query)
        req['query'] = bson.Binary(bson.BSON.encode(query))
        if map is not None:
            if map_name is None: map_name = map.func_name
            req.update(map_text=func_text(map), map_name=map_name)
        if reduce is not None:
            if reduce_name is None: reduce_name = reduce.func_name
            req.update(reduce_text=func_text(reduce), reduce_name=reduce_name)
        if finalize is not None:
            if finalize_name is None: finalize_name = finalize.func_name
            req.update(finalize_text=func_text(finalize), finalize_name=finalize_name)
        r = req_bson(socket, req)
        self._job_ids.append(r['job_id'])
        return r

    def _calculate_id_max(self):
        db = ZM.doc_session.db
        q = { self._id_key: {'$gt': self.id_min } }
        id_max = 0
        for cname in self._inputs:
            c = db[cname]
            if self._id_key != '_id':
                c.ensure_index(self._id_key)
            log.debug('finding last_id from %s query %s' % (cname, q))
            for last_item in c.find().sort(self._id_key, -1):
                log.debug('    taking max of: %s %s' % (id_max, last_item[self._id_key]))
                id_max = max(id_max, last_item[self._id_key])
                break
        if self._limit:
            id_max = min(self.id_min + self._limit, id_max)
        return id_max

    def _start_jobs(self, socket, full):
        '''Kick off all the MR jobs required to execute this task. If 'full' is
        specified, do complete recalculation of the aggregate.'''
        raise NotImplementedError, '_start_jobs'
