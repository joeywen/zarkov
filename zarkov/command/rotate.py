import os
import gzip
import logging
from datetime import datetime
from contextlib import closing

import bson
from pymongo.errors import OperationFailure

from .base import Command
from zarkov import model as M

log = logging.getLogger(__name__)

class RotateCommand(Command):
    name='rotate'
    help=(
        'zcmd rotate <dump directory>\n'
        'Rotate all our event collections, dumping and dropping the oldest')
    min_args = max_args = 1

    def run(self):
        db = M.doc_session.db
        s_event_logs = set(
            n for n in db.collection_names() if n.startswith('event.'))
        def lognum(n): return int(n.rsplit('.')[-1])
        if s_event_logs:
            last_event_log = max(s_event_logs, key=lognum)
            last_num = lognum(last_event_log)
            event_logs = [  'event.%d' % i for i in range(last_num+1) ]
        else:
            event_logs = [ ]
        log.info('Old event logs:')
        if event_logs:
            for el in event_logs:
                if el in s_event_logs:
                    log.info('... %s', el)
                else:
                    log.info('... %s (missing)', el)
        else:
            log.info('... (no old event logs found)')

        # Perform the rotation
        from_names = [ 'event' ] + event_logs
        to_names = event_logs + [ 'event.%d' % len(event_logs) ]
        for f,t in reversed(zip(from_names, to_names)):
            log.info('%s => %s', f, t)
            try:
                db[f].rename(t)
                continue
            except OperationFailure:
                log.info('  First rename failed (not fatal).', exc_info=True)
                try:
                    db.create_collection(f)
                except OperationFailure as e2:
                    log.info('  create_collection failed', exc_info=True)
                    pass
            db[f].rename(t)

        # Perform the dumps
        for cname in to_names[self.options.num_event_logs:]:
            path = os.path.join(
                self.args[-1],
                '%s.%s.bson.gz' % (cname, datetime.utcnow().isoformat()))
            log.info('Dump %s => %s', cname, path)
            with closing(gzip.open(path, 'wb')) as fp:
                for x in db[cname].find():
                    fp.write(bson.BSON.encode(x))

        # Perform the drops
        for cname in to_names[self.options.num_event_logs:]:
            log.info('Drop %s', cname)
            db[cname].drop()
