import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from kettle import settings
from kettle.db import session

def create_engine(engine_string=None, *args, **kwargs):
    if engine_string is None:
        engine_string = settings.ENGINE_STRING
    if engine_string.startswith('sqlite'):
        # Make SQLite thread safe
        connect_args = kwargs.get('connect_args', {})
        connect_args['check_same_thread'] = False
        kwargs['connect_args'] = connect_args
    return sqlalchemy.create_engine(engine_string, *args, **kwargs)

engine = create_engine()

Base = declarative_base()

def make_session(bind=None):
    if bind is None:
        bind = engine
    session.Session = scoped_session(sessionmaker(bind=bind))


def drop_all(engine_=None):
    metadata_task('drop_all', engine_)

def create_all(engine_=None):
    metadata_task('create_all', engine_)

def metadata_task(fn_name, engine_):
    from kettle.rollout import Rollout
    if engine_ is None:
        engine_ = engine
    metadata_fn = getattr(Rollout.metadata, fn_name)
    metadata_fn(engine_)
