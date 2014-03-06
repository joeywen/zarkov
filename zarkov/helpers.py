'''Some helpers for testing, mainly.'''
import logging

from zarkov import task
from zarkov import model as ZM

log = logging.getLogger(__name__)

class PerfTestAgg(task.Task):

    def _start_jobs(self, socket, full):
        self.start_xmr(
            socket, full, map=self.xmap, reduce=self.xreduce_sum,
            name='perftest')

    def xmap(objs):
        for o in objs:
            yield dict(
                c='perftest.sum_by_second',
                k='%.02d' % o['timestamp'].second,
                v=dict(v=1))
            yield dict(
                c='perftest.sum_by_second_ts',
                k=o['timestamp'].replace(microsecond=0).isoformat(),
                v=dict(v=1))
            yield dict(
                c='perftest.sum_by_minute',
                k='%.02d' % o['timestamp'].minute,
                v=dict(v=1))
            yield dict(
                c='perftest.sum_by_hour',
                k='%.02d' % o['timestamp'].hour,
                v=dict(v=1))
            yield dict(
                c='perftest.sum_by_date',
                k=o['timestamp'].date().isoformat(),
                v=dict(v=1))
            yield dict(
                c='perftest.sum_by_month',
                k=o['timestamp'].replace(day=1).date().isoformat(),
                v=dict(v=1))
            yield dict(
                c='perftest.sum_by_year',
                k='%.02d' % o['timestamp'].year,
                v=dict(v=1))
    def xreduce_sum(collection, key, values):
        from collections import defaultdict
        result = defaultdict(int)
        for v in values:
            for k,vv in v.items():
                result[k] += vv
        return result
        

def setup_time_aggregates():
    ZM.AggDef.query.remove()
    ZM.AggDef(
        name='perftest',
        task_name='zarkov.helpers.PerfTestAgg')
    ZM.orm_session.flush()

