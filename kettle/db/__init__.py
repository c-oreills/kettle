from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from kettle import settings
from kettle.db import session

engine = create_engine(settings.ENGINE_STRING)

Base = declarative_base()

def make_session(bind=None):
    if bind is None:
        bind = engine
    session.Session = scoped_session(sessionmaker(bind=bind))


def drop_all():
    metadata_task('drop_all')

def create_all():
    metadata_task('create_all')

def metadata_task(fn_name):
    from kettle.rollout import Rollout
    metadata_fn = getattr(Rollout.metadata, fn_name)
    metadata_fn(engine)
