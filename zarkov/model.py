'''The Zarkov data model.'''
import logging

from datetime import datetime

import gevent
import pymongo.errors

from ming import Session, collection, Field
from ming import schema as S
from ming.orm import ContextualORMSession, session

log = logging.getLogger(__name__)

doc_session = Session.by_name('zarkov')
orm_session = ContextualORMSession(
    lambda:id(gevent.getcurrent), doc_session)

# Singleton collection
idgen = collection(
    'idgen', doc_session,
    Field('_id', int, if_missing=0),
    Field('inc', int))

# Things we want to track -- the 'fact table' from an OLAP perspective
event = collection(
    'event', doc_session,
    Field('_id', int),
    Field('timestamp', datetime, if_missing=datetime.utcnow),
    Field('type', str),
    Field('context', {str:None}),
    Field('extra', None))

# Aggregations defined over (by default) the 'event' collection
agg_def = collection(
    'agg_def', doc_session,
    Field('_id', S.ObjectId()),
    # Name of the aggregation
    Field('name', str, unique=True),
    # Field used to detect whether we need to update the agg
    Field('id_key', str, if_missing='_id'),
    # Input collections to aggregate
    Field('inputs', [str], if_missing=lambda:['event', 'event.0']),
    # Pointer to python Aggregation subclass
    Field('task_name', str),
    # Flag to indicate whether the agg should be real-time
    Field('realtime', bool, if_missing=False),
    # Max ID from the last aggregation
    Field('last_id', int, if_missing=0),
    # Last ID timestamp
    Field('last_id_timestamp', datetime, if_missing=datetime.utcnow))


class IdGen(object):

    @classmethod
    def get_ids(cls, inc=1):
        obj = cls.query.find_and_modify(
            query={'_id':0},
            update={
                '$inc': dict(inc=inc),
                },
            upsert=True,
            new=True)
        return range(obj.inc - inc, obj.inc)

class Event(object): pass

class AggDef(object):

    @property
    def src_collections(self):
        return [ doc_session.db[o] for o in self.inputs]

    @classmethod
    def upsert(cls, name, **kwargs):
        obj = cls.query.get(name=name)
        if obj is None:
            obj = cls(name=name)
            try:
                session(cls).flush(obj)
            except pymongo.errors.DuplicateKeyError: # pragma no cover
                obj = cls.query.get(name=name)
        for k, v in kwargs.items():
            setattr(obj, k, v)
        return obj

    def task(self, *args, **kwargs):
        smod, task = self.task_name.rsplit('.', 1)
        cur = __import__(smod, fromlist=[str(task)])
        TaskClass = getattr(cur, task)
        return TaskClass(self, *args, **kwargs)

    def incremental(self, zmr_sock, limit=None):
        t = self.task(limit=limit)
        r = t.run(zmr_sock)
        return r

    def full(self, zmr_sock, limit=None):
        t = self.task(limit=limit, id_min=0)
        return t.run(zmr_sock, full=True)

orm_session.mapper(IdGen, idgen)
orm_session.mapper(Event, event)
orm_session.mapper(AggDef, agg_def)
