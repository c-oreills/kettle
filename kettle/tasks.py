from datetime import datetime
from threading import Event
from time import sleep
import traceback

import logbook
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship, backref

from db import Base, session
from db.fields import JSONEncodedDict
from steps import run_step_thread, thread_wait, friendly_step_str, friendly_step_html

def action_fn(action):
    def do_action(instance):
        action_start_dt = getattr(instance, '%s_start_dt' % (action,), None)
        if not action_start_dt:
            instance.call_and_record_action(action)
        else:
            raise Exception('%s already started at %s' % 
                    (action, action_start_dt))
    return do_action


class ReversibleTask(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    type = Column(String(50), nullable=False)
    rollout_id = Column(Integer, ForeignKey('rollout.id'))
    state = Column(JSONEncodedDict(1000))

    run_start_dt = Column(DateTime)
    run_error = Column(String(500))
    run_error_dt = Column(DateTime)
    run_return = Column(String(500))
    run_return_dt = Column(DateTime)
    run_traceback = Column(String(500))

    rollback_start_dt = Column(DateTime)
    rollback_error = Column(String(500))
    rollback_error_dt = Column(DateTime)
    rollback_return = Column(String(500))
    rollback_return_dt = Column(DateTime)
    rollback_traceback = Column(String(500))

    rollout = relationship('Rollout', backref=backref('tasks', order_by=id))

    run = action_fn('run')
    rollback = action_fn('rollback')

    @declared_attr
    def __mapper_args__(cls):
        # Implementing single table inheritance
        if cls.__name__ == 'ReversibleTask':
            return {'polymorphic_on': cls.type,}
        else:
            return {'polymorphic_identity': cls.__name__}

    def __init__(self, rollout_id, *args, **kwargs):
        self.rollout_id = rollout_id
        self.state = {}
        self._init(*args, **kwargs)

    def call_and_record_action(self, action):
        setattr(self, '%s_start_dt' % (action,), datetime.now())
        self.save()
        # action_fn should be a classmethod, hence getattr explicitly on type
        action_fn = getattr(type(self), '_%s' % (action,))
        try:
            action_return = action_fn(self.state)
        except Exception, e:
            setattr(self, '%s_error' % (action,), e.message)
            setattr(self, '%s_traceback' % (action,), repr(traceback.format_exc()))
            setattr(self, '%s_error_dt' % (action,), datetime.now())
            raise
        else:
            setattr(self, '%s_return' % (action,), action_return)
            setattr(self, '%s_return_dt' % (action,), datetime.now())
        finally:
            self.save()

    def save(self):
        if self not in session.Session:
            session.Session.add(self)
        session.Session.commit()

    def _init(self, *args, **kwargs):
        pass

    @classmethod
    def _run(cls, state):
        pass

    @classmethod
    def _rollback(cls, state):
        pass

    @classmethod
    def friendly_str(cls, *args, **kwargs):
        return '%s(%s, %s)' % (cls.__name__, ', '.join(map(repr, args)), ', '.join(['%s=%s' % (k, v) for k, v in kwargs.iteritems()]))

    @classmethod
    def friendly_html(cls, *args, **kwargs):
        return cls.friendly_str(*args, **kwargs)


class Task(object):
    def __init__(self, rollout_id, *args, **kwargs):
        self.rollout_id = rollout_id
        self._init(*args, **kwargs)

    def _init(self, *args, **kwargs):
        pass

    def run(self):
        pass

    @classmethod
    def friendly_str(cls, *args, **kwargs):
        return '%s(%s)' % (cls.__name__, ', '.join(
            map(repr, args) + ['%s=%s' % (k, v) for k, v in kwargs.iteritems()]))

    @classmethod
    def friendly_html(cls, *args, **kwargs):
        return cls.friendly_str(*args, **kwargs)


class ParallelStepsTask(Task):
    def _init(self, steps):
        self.steps = steps

    def run(self):
        self.threads = []
        self.parallel_abort = Event()
        for step in self.steps:
            thread = run_step_thread(self.rollout_id, step, self.parallel_abort)
            self.threads.append(thread)
        for thread in self.threads:
            thread_wait(thread, self.parallel_abort)
        if self.parallel_abort.is_set():
            raise Exception('Caught exception while executing tasks in parallel')

    @classmethod
    def friendly_str(cls, steps):
        return 'Execute in parallel:%s' %\
                ''.join(['\n\t%s' % friendly_step_str(step) for step in steps])

    @classmethod
    def friendly_html(cls, steps):
        return 'Execute in parallel:<ul>%s</ul>' %\
                ''.join(['<li>%s</li>' % friendly_step_html(step) for step in steps])


class DelayTask(Task):
    def _init(self, minutes, **kwargs):
        self.minutes = minutes

    def run(self):
        logbook.info('Waiting for %sm' % (self.minutes,),)
        sleep(self.minutes*60)

    @classmethod
    def friendly_str(cls, minutes, **kwargs):
        return 'Delay for %s mins' % (minutes,)
