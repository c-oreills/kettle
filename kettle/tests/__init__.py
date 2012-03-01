from sqlalchemy import create_engine
from unittest import TestCase

from kettle import rollout, tasks # Needed for metadata
from kettle.db import ENGINE_STRING, make_session

engine = create_engine('%s_test' % ENGINE_STRING)

rollout.Rollout.metadata.drop_all(engine)
rollout.Rollout.metadata.create_all(engine)

class AlchemyTestCase(TestCase):
    def _pre_setup(self):
        # connect to the database
        self.connection = engine.connect()

        # begin a non-ORM transaction
        self.trans = self.connection.begin()

        # bind thread-local Session to the connection
        make_session(self.connection)

    def __call__(self, result=None):
        """
        Wrapper around default __call__ method to perform common test set up.
        This means that user-defined Test Cases aren't required to include a
        call to super().setUp().
        """
        try:
            self._pre_setup()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            import sys
            result.addError(self, sys.exc_info())
            return
        super(AlchemyTestCase, self).__call__(result)
        try:
            self._post_teardown()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            import sys
            result.addError(self, sys.exc_info())
            return

    def _post_teardown(self):
        # rollback - everything that happened with the Session above (including
        # calls to commit()) is rolled back.
        self.trans.rollback()
        self.connection.close()
