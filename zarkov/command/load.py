import os
import gzip
import time
import logging
import tempfile

import bson

from .base import Command

log = logging.getLogger(__name__)

class LoadCommand(Command):
    name='load'
    help=("load <database> <collection> <text file>...\n"
          "Load a text file into a MongoDB colleciton without further processing,"
          " one line at a time")
    
    min_args=3
    max_args=None
    
    def run(self):
        begin = time.time()
        database, collection = self.args[:2]
        ndocs = 0
        for ifn in self.args[2:]:
            bsonfile, ndocs_ = self._slurp_file(ifn)
            ndocs += ndocs_
            cmd = self.mongorestore_command(
                database=database,
                bsonfile=bsonfile, collection=collection)
            log.info('Restore command: %r', cmd)
            assert 0 == os.system(cmd)
            os.remove(bsonfile)
        elapsed = time.time() - begin
        log.info('%d docs imported in %d secs, %d docs/s',
                 ndocs, elapsed, ndocs / elapsed)

    def _slurp_file(self, fn):
        if fn.endswith('.gz'):
            ifp = gzip.open(fn)
        else:
            ifp = open(fn)
        try:
            fd, bsonfile = tempfile.mkstemp(suffix='.bson')
            log.info('Slurping gz to bsonfile %s', bsonfile)
            with open(bsonfile, 'wb') as ofp:
                ndocs = self._log2bson(ifp, ofp)
            return bsonfile, ndocs
        finally:
            ifp.close()

    def _log2bson(self, fp_in, fp_out):
        begin = time.time()
        for i, line in enumerate(fp_in):
            if (i+1) % 100000 == 0:
                lps = i / (time.time() - begin)
                log.info('%d lps @ %d lines', lps, i+1)
            fp_out.write(bson.BSON.encode(dict(
                        _id=bson.ObjectId(),
                        line=line)))
        return i

