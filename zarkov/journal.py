'''Provide a write-ahead log (WAL) for zarkov in case mongodb goes down
'''
import os
import bson
import glob
import struct
import mmap
import logging
from random import randint
from itertools import izip
from datetime import datetime
from contextlib import closing

import gevent.queue

from zarkov import model

log = logging.getLogger(__name__)

EOF = {}
EOF_BSON=bson.BSON.encode(EOF)

class JournalWriter(object):
    '''Object which will create/use the WAL and store events to mongodb whenever
    it is possible. The JournalWriter will automatically 'catch up' if mongodb
    goes down and then returns.'''

    # Number of seconds to wait before attempting to reconnect to mongodb if it
    # goes down
    RETRY_DELAY=10
    # Number of seconds to be idle before flushing everything in the log to mongodb
    IDLE_FLUSH=1

    def __init__(self, options, journal_dir, start_greenlets=True):
        self._options = options
        self._journal_dir = journal_dir
        self._journal_size = None
        self._fn = self._mm = None
        self._bytes_written = 0
        self._commit_queue = gevent.queue.Queue()
        self._last_commit = datetime.utcnow()
        self._replay()
        self._open_writer()
        if start_greenlets:
            self.greenlets = [
                gevent.spawn_link_exception(self._commit_greenlet),
                gevent.spawn_link_exception(self._idle_greenlet) ]

    def kill(self):
        gevent.killall(self.greenlets)

    def __call__(self, obj):
        '''Write an event to the log. May trigger a log flush to mongodb.'''
        obj_buf = bson.BSON.encode(obj)
        obj_len = len(obj_buf)
        if obj_len + self._bytes_written > self._journal_size - len(EOF_BSON):
            self._enqueue_commit()
        self._mm.write(obj_buf)
        self._mm.write(EOF_BSON)
        self._mm.seek(-len(EOF_BSON), os.SEEK_CUR)
        if self._options.flush_mmap:
            log.info('Flushing mmap for journal')
            self._mm.flush()
        self._bytes_written += obj_len
        gevent.sleep(0)

    def _open_writer(self):
        oldfn = self._fn
        if self._mm:
            self._mm.close()
        if not os.path.exists(self._journal_dir):
            os.makedirs(self._journal_dir)
        fns = [
            os.path.join(self._journal_dir, fn)
            for fn in os.listdir(self._journal_dir)
            if fn.startswith('_') ]
        if not fns:
            fns = list(allocate_journals(
                    self._journal_dir, self._options.journal_file_size,
                    self._options.journal_min_files))
        self._fn =  os.path.join(
            self._journal_dir, 'z_%s.j' % datetime.utcnow().isoformat())
        os.rename(fns[0], self._fn)
        fp = open(self._fn, 'r+b')
        self._mm = mmap.mmap(fp.fileno(), 0)
        self._mm[:len(EOF_BSON)] = EOF_BSON
        self._bytes_written = 0
        self._journal_size = os.stat(self._fn).st_size
        return oldfn

    def _idle_greenlet(self):
        log.info('Entering idle greenlet')
        while True:
            elapsed = datetime.utcnow() - self._last_commit
            elapsed_s = (elapsed.seconds + elapsed.microseconds/1000.)
            if elapsed_s < self.IDLE_FLUSH:
                gevent.sleep(self.IDLE_FLUSH - elapsed_s)
            self._enqueue_commit()

    def _commit_greenlet(self):
        log.info('Entering commit greenlet')
        # this loop never exits
        for fn in self._commit_queue:
            log.info('flushing %s' % fn)
            self._try_flush(fn)
        log.info('Leaving commit greenlet loop')

    def _enqueue_commit(self):
        if self._bytes_written:
            oldfn = self._open_writer()
            self._commit_queue.put(oldfn)
        self._last_commit = datetime.utcnow()

    def _try_flush(self, fn):
        bson_error = False
        try:
            coll = model.doc_session.db.event
            with open(fn, 'rb') as fp:
                with closing(mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ)) as mm:
                    objects = []
                    try:
                        for o in decode_bson_objects(mm, sentinel=EOF):
                            objects.append(o)
                    except (bson.errors.BSONError, UnicodeError, MemoryError):
                        log.exception('Error reading journal file %s  Got %s objects before error' % (fn, len(objects)))
                        bson_error = True
                    if objects:
                        ids = model.IdGen.get_ids(len(objects))
                        for id, o in izip(ids, objects):
                            o['_id'] = id
                        #log.info('inserting into mongo')
                        coll.insert(objects, safe=True)
                    else:
                        log.info('no objects exist when flushing journal file.  Probably an empty pre-allocated file.  %s' % fn)
        except ValueError as e:
            # handle specific mmap opening error
            if str(e) == 'mmap offset is greater than file size' and os.path.getsize(fn) == 0:
                log.warn('Ignoring 0-sized journal file %s' % fn)
                os.remove(fn)
                return
            else:
                raise
        except:
            log.exception('Error flushing %s, queue for retry in %ds', fn, self.RETRY_DELAY)
            gevent.spawn_later(self.RETRY_DELAY, self._commit_queue.put, fn)
            return

        available_files = glob.glob(os.path.join(self._journal_dir, '_z_*'))
        if bson_error:
            new_fn = os.path.join(
                    self._journal_dir,
                    'bson_error_%s_%d' % (
                        datetime.utcnow().isoformat(), randint(0,100)))
            log.info('Renaming %s to %s due to bson parsing error.  Perhaps some more data is still recoverable' % (fn, new_fn))
            os.rename(fn, new_fn)
        elif len(available_files) > self._options.journal_min_files:
            os.remove(fn)
        else:
            os.rename(fn, os.path.join(
                    self._journal_dir,
                    '_z_%s_%d' % (
                        datetime.utcnow().isoformat(), randint(0,100))))

    def _replay(self):
        '''Replay all journal files, in order.'''
        filenames = sorted([
                fn for fn in os.listdir(self._journal_dir)
                if fn.startswith('z_') ])
        for fn in filenames:
            log.info('Request replay of %s', fn)
            self._commit_queue.put(
                os.path.join(self._journal_dir, fn))

def allocate_journals(dirname, size, count):
    for i in xrange(count):
        fn = os.path.join(dirname, '_z_%s_%d' % (
                datetime.utcnow().isoformat(), i))
        log.info('allocating %s' % fn)
        with open(fn, 'wb') as fp:
            fp.write('\x00' * size)
            fp.seek(0)
            fp.write(EOF_BSON)
        yield fn

def decode_bson_objects(data, sentinel):
    '''
    Decode consecutive BSON data.  Sentinel indicates end of data.
    Meaningless data may exist after the sentinel, since data files are re-used.
    '''
    pos = 0
    while True:
        dlen = struct.unpack('<i', data[pos:pos+4])[0]
        obj = bson.BSON(data[pos:pos+dlen]).decode()
        if obj == sentinel:
            break
        pos += dlen
        yield obj
