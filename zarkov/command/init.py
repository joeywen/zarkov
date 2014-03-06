from .base import Command
from zarkov import model as M

class InitializeCommand(Command):
    name='init'
    help='Initialize the zarkov DB'
    max_args=0

    def run(self):
        # Drop any existing zarkov database
        db = M.doc_session.db
        db.connection.drop_database(db)
