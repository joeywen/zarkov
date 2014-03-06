from .base import Command

# Main event logger
from .eventlog import EventLogCommand

# TCP-to-Zarkov gateway intended for logfile loading
from .logstream import LogStreamCommand

# Event load balancer
from .loadbalance import LoadBalanceCommand

# JSON web service to provide aggregate data
from .web_server import WebServerCommand

# Simple no-op command to check configs
from .check_config import CheckConfigCommand

# Run an ipython shell as a command
from .shell import ShellCommand

# Run a script with the Zarkov context
from .script import ScriptCommand

# Run an aggregation
from .aggregate import AggregateCommand

# Display help on commands
from .help import HelpCommand

# Zarkov map-reduce router/worker
from .zmr import RouterCommand, WorkerCommand

# Initialize the Zarkov database (this will drop all events and aggregates)
from .init import InitializeCommand

# Rotate the Zarkov event logs
from .rotate import RotateCommand

# Gateway for Zarkov to send web requests as events
from .web_event import WebEventCommand

# Authenticating proxy
from .proxy import ProxyCommand

# Create lots of events -- kind of an 'ab' for Zarkov
from .perftest import PerformanceTestCommand

# Import a collection as a set of Zarkov events (pending deprecation)
from .import_ import ImportCommand

# Load a text file into mongodb without further processing (pending deprecation)
from .load import LoadCommand

