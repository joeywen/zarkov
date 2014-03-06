#!/usr/bin/env python
'''Simple script that listens to stdin for newline-terminated JSON and echoes it
to a PUSH socket connected to Zarkov's event server.
'''
import sys
import json
from datetime import datetime

import zmq
import bson

USAGE='%s <zarkov BSON socket> [ <shared key> ]' % sys.argv[0]

def main():
    if len(sys.argv) not in [2,3]:
        print USAGE
        sys.exit(2)
    if len(sys.argv) == 3:
        shared_key = sys.argv[2]
        if shared_key.startswith('@'):
            shared_key = open(shared_key[1:]).read()
    else:
        shared_key = None
    context = zmq.Context()
    sock = context.socket(zmq.PUSH)
    sock.connect(sys.argv[1])
    for line in sys.stdin:
        line = line.rstrip()
        if not line: continue
        obj = bson_from_json(line)
        if shared_key:
            sock.send(shared_key, zmq.SNDMORE)
        sock.send(obj)

def bson_from_json(msg):
    def object_hook(dct):
        if '$datetime' in dct:
            return datetime.utcfromtimestamp(dct['$datetime'])
        return dct
    obj = json.loads(msg, object_hook=object_hook)
    return bson.BSON.encode(obj)

if __name__ == '__main__':
    main()
