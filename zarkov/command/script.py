import sys
from .base import Command

class ScriptCommand(Command):
    name='script'
    help='Run a script'
    min_args=1
    max_args=None

    def run(self):
        with open(self.args[0]) as fp:
            ns = dict(
                self=self,
                __name__='__main__')
            sys.argv = self.args
            exec fp in ns

        
            
                       
