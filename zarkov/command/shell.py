from .base import Command

class ShellCommand(Command):
    name='shell'
    help='Run an ipython shell'

    def run(self):
        import IPython
        from zarkov import model as ZM
        ns = dict(ZM=ZM)
        IPython.embed(user_ns=ns)

        
            
                       
