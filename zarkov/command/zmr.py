import os
import shutil
import logging
import multiprocessing

import gevent
from gevent_zeromq import zmq

from zarkov.zmr import router, worker

from .base import Command

log = logging.getLogger(__name__)

class RouterCommand(Command):
    name='zmr-router'
    help=("Zarkov Map-Reduce Router")
    
    min_args=max_args=0

    def run(self):
        # Start router
        context = zmq.Context()
        log.info('%s startup', self.name)
        rtr = router.ZMRRouter(context, self.options)
        rtr.start()
        rtr.serve_forever()

class WorkerCommand(Command):
    name='zmr-worker'
    help=("Zarkov Map-Reduce Worker")
    
    min_args=max_args=0

    def run(self):
        router_addr = self.options.zmr['req_uri']
        nproc = self.options.zmr['processes_per_worker'] or multiprocessing.cpu_count()
        worker.ZMRWorker.manage_workers(router_addr, nproc)

class CleanCommand(Command):
    name='zmr-clean'
    help=("Zarkov Temp File Cleanup")

    min_args=max_agrs=0

    def run(self):
        job_root = self.options.zmr['job_root']
        if os.path.exists(job_root):
            shutil.rmtree(job_root)
