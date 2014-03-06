import os
import logging
import tempfile
from datetime import datetime, timedelta

import bson

from zarkov import model as ZM

from .base import Command

log = logging.getLogger(__name__)

class ImportCommand(Command):
    name='import'
    help=("import <database> <collection>\n"
          "Import the 'value' field of a collection as Zarkov events")
    
    min_args=max_args=2

    def run(self):
        database, cname = self.args[:2]
        conn = ZM.doc_session.db.connection
        ZM.event.m.ensure_indexes()
        fd, bsonfile = tempfile.mkstemp(suffix='.bson')
        os.close(fd)
        restore_command = self.mongorestore_command(
            bsonfile=bsonfile, collection='event')
        log.info('Restore command: %r', restore_command)

        # Import the events
        db = ZM.doc_session.db
        with open(bsonfile, 'wb') as fp:
            for obj in conn[database][cname].find():
                event = obj['value']
                event.setdefault('z_timestamp', None)
                fp.write(bson.BSON.encode(event))
        os.system(restore_command)
        os.remove(bsonfile)

        # Set the timestamps for the events as 10s in the future
        ts = datetime.utcnow() + timedelta(seconds=10)
        db.event.update({'z_timestamp':None}, {'$set': {'z_timestamp':ts}}, multi=True)
