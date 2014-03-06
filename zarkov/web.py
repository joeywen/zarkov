import json
import time
import logging
from pprint import pformat
from dateutil import parser
from collections import defaultdict

import pymongo
from webob import Request, Response, exc
from formencode.schema import Schema
from formencode import validators as V
from formencode.foreach import ForEach
from formencode import Invalid
from formencode.variabledecode import variable_decode

from zarkov import model

log = logging.getLogger(__name__)

class WebApp(object):

    def __init__(self):
        self._db = model.doc_session.db
        self._handlers = dict(
            q=LegacyHandler(
                self.query_time_series,
                Schema(
                    p=V.UnicodeString(if_missing='zagg.'),
                    c=V.UnicodeString(required=True),
                    b=V.UnicodeString(if_missing=None),
                    e=V.UnicodeString(if_missing=None))),
            qs=Handler(
                self.query_time_series,
                Schema(
                    q=ForEach(
                        Schema(
                            n=V.UnicodeString(required=True),
                            p=V.UnicodeString(if_missing='zagg.'),
                            c=V.UnicodeString(required=True),
                            b=V.UnicodeString(if_missing=None),
                            e=V.UnicodeString(if_missing=None)),
                        convert_to_list=True))),
            qr=Handler(
                self.query_ranked,
                Schema(
                    q=ForEach(
                        Schema(
                            n=V.UnicodeString(required=True),
                            p=V.UnicodeString(if_missing='zagg.'),
                            c=V.UnicodeString(required=True),
                            f=JSONValidator(required=True),
                            limit=V.Int(max=100),
                            sort=V.UnicodeString(if_missing='value.rank')),
                        convert_to_list=True))),
            collections=Handler(self.list_collections))

    def __call__(self, environ, start_response):
        req = Request(environ)
        try:
            h = self._handlers[req.path_info[1:].split('/', 1)[0]]
            res = h(req)
        except KeyError:
            res = exc.HTTPNotFound()
        except exc.HTTPError, err:
            res = err
        return res(environ, start_response)

    def query_time_series(self, q):
        result = {}
        for query in q:
            name = query['n']
            prefix = query['p']
            collection = query['c']
            begin = query['b']
            end = query['e']

            filter = defaultdict(dict)
            if begin: filter['_id']['$gte'] = begin
            if end: filter['_id']['$lte'] = end

            q_local = self._db[prefix + collection].find(filter)
            q_local = q_local.sort('_id')
            result[name] = data = defaultdict(list)
            for d in q_local:
                ts = d['_id']
                try:
                    datestr = d['_id'].split('/')[-1]
                    if ':' in datestr or '-' in datestr:
                        dt = parser.parse(datestr)
                        ts = time.mktime(dt.timetuple()) * 1000
                except:
                    pass
                for fld, value in d['value'].items():
                    data[fld].append([ts, value])
        return result

    def query_ranked(self, q):
        result = {}
        for query in q:
            name = query['n']
            prefix = query['p']
            collection = query['c']
            filter = query['f']
            limit = query['limit']
            sort = query['sort']

            q_local = self._db[prefix + collection].find(filter)
            if sort.startswith('-'):
                sort = sort[1:]
                sort_order = pymongo.DESCENDING
            else:
                sort_order = pymongo.ASCENDING
            q_local = q_local.sort(sort, sort_order)
            q_local = q_local.limit(limit)
            result[name] = list(q_local)
        return result

    def list_collections(self):
        return dict(options=[
                cn.split('.', 1)[-1] for cn in self._db.collection_names()
                if cn.startswith('zagg.')])

class Handler(object):

    def __init__(self, action, schema=None):
        self.action = action
        self.schema = schema

    def __call__(self, req):
        try:
            kwargs = self.prep_kwargs(req)
            r = self.action(**kwargs)
            res = Response()
            res.content_type = 'application/json'
            res.body = json.dumps(r, default=str)
            return res
        except Invalid:
            log.exception('bad request: params=%s', pformat(req.params))
            return exc.HTTPBadRequest()
        except:
            log.exception('500 error: params=%s', pformat(req.params))
            return exc.HTTPServerError()

    def prep_kwargs(self, req):
        params = variable_decode(req.params)
        if self.schema:
            params = self.schema.to_python(params, None)
        return params

class LegacyHandler(Handler):

    def prep_kwargs(self, req):
        q = []
        for k,v in req.params.items():
            v = json.loads(v)
            v = self.schema.to_python(v, None)
            v['n'] = k
            q.append(v)
        return dict(q=q)

class JSONValidator(V.FancyValidator):

    def _to_python(self, value, state=None):
        try:
            return json.loads(value)
        except ValueError:
            raise Invalid('Bad json', value, state)

    def _from_python(self, value, state=None):
        return json.dumps(value)
