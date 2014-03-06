from .base import Command

class HelpCommand(Command):
    name='help'
    help='Print this message'
    max_args=None

    def run(self):
        if self.args:
            names = self.args
            detail = True
        else:
            names = sorted(self._registry.keys())
            detail = False
        for name in names:
            self._print_help(name, detail)

    def _print_help(self, name, detail=False):
        try:
            command = self._registry[name]
        except KeyError:
            print '%s: NOT FOUND' % name
            return
        if detail:
            print '--'
        header = name
        if command.min_args == command.max_args:
            header += ' (exactly %d args)' % command.min_args
        else:
            if command.min_args:
                header += ' (min %d args)' % command.min_args
            if command.max_args is not None:
                header += ' (max %d args)' % command.max_args
        print header
        print '    ' + command.help.replace('\n', '\n    ')
        if detail:
            print
            if command.detailed_help:
                print command.detailed_help
            
