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
