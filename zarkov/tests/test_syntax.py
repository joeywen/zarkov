import os.path
from subprocess import Popen, PIPE
import sys

toplevel_dir = os.path.abspath(os.path.dirname(__file__) + "/../..")

def run(cmd):
    proc = Popen(cmd, shell=True, cwd=toplevel_dir, stdout=PIPE, stderr=PIPE)
    # must capture & reprint stdount, so that nosetests can capture it
    (stdout, stderr) = proc.communicate()
    sys.stdout.write(stdout)
    sys.stderr.write(stderr)
    return proc.returncode

find_py = "find zarkov/ -name '*.py'"

# a recepe from itertools doc
from itertools import izip_longest
def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def test_pyflakes():
    # skip some that aren't critical errors
    skips = [
        'imported but unused',
        'redefinition of unused',
        'assigned to but never used',
        '__version__',
    ]
    proc = Popen(find_py, shell=True, cwd=toplevel_dir, stdout=PIPE, stderr=PIPE)
    (find_stdout, stderr) = proc.communicate()
    sys.stderr.write(stderr)
    assert proc.returncode == 0, proc.returncode

    # run pyflakes in batches, so it doesn't take tons of memory
    error = False
    all_files = [f for f in find_stdout.split('\n') if f.strip()]
    for files in grouper(20, all_files, fillvalue=''):
        cmd = "pyflakes " + ' '.join(files) + " | grep -v '" + "' | grep -v '".join(skips) + "'"
        #print 'Command was: %s' % cmd
        retval = run(cmd)
        if retval != 1:
            print
            #print 'Command was: %s' % cmd
            print 'Returned %s' % retval
            error = True

    if error:
        raise Exception('pyflakes failure, see stdout')