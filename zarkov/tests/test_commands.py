from contextlib import contextmanager
import sys
from StringIO import StringIO

from zarkov import config
import zarkov.command


@contextmanager
def stdout_redirected(new_stdout):
    save_stdout = sys.stdout
    sys.stdout = new_stdout
    try:
        yield None
    finally:
        sys.stdout = save_stdout

def make_command(cmd, *args):
    # somewhat duplicated from scripts/zcmd
    args = list(args)
    options, args = config.configure(args)
    Command = zarkov.command.Command.lookup(cmd)
    return Command(options, *args)

def test_help():
    command = make_command('help')

    capture = StringIO()
    with stdout_redirected(capture):
        command.run()
    assert 'Run the specified aggregation' in capture.getvalue(), capture.getvalue()

def test_help_detail():
    command = make_command('help', 'logstream')

    capture = StringIO()
    with stdout_redirected(capture):
        command.run()
    assert 'Run the specified aggregation' not in capture.getvalue(), capture.getvalue()
    assert 'logstream <eventlog_uri> <plugin>:<port>' in capture.getvalue(), capture.getvalue()
    assert 'Listens on a given port' in capture.getvalue(), capture.getvalue()

def test_help_invalid():
    command = make_command('help', 'asdfasdf')

    capture = StringIO()
    with stdout_redirected(capture):
        command.run()
    assert 'NOT FOUND' in capture.getvalue(), capture.getvalue()
