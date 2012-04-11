from datetime import datetime
from threading import Event, Thread
from time import sleep
import traceback

import logbook
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship, backref

from db import Base, session
from db.fields import JSONEncodedDict

from log_utils import get_thread_handlers, inner_thread_nested_setup

def action_fn(action):
    def do_action(instance):
        action_start_dt = getattr(instance, '%s_start_dt' % (action,), None)
        if not action_start_dt:
            instance.call_and_record_action(action)
        else:
            raise Exception('%s already started at %s' % 
                    (action, action_start_dt))
    return do_action

def thread_wait(thread, abort_event):
    try:
        while True:
            if abort_event.is_set():
                pass # TODO: Set some kinda timeout
            if thread.is_alive():
                thread.join(1)
            else:
                break
    except Exception:
        print traceback.format_exc()
        abort_event.set()


class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    type = Column(String(50), nullable=False)
    rollout_id = Column(Integer, ForeignKey('rollout.id'))
    parent_id = Column(Integer, ForeignKey('task.id'))
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
    parent = relationship('Task', backref=backref('children', order_by=id))

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
            action_return = action_fn(self.state, self.children)
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
    def _run(cls, state, children):
        pass

    @classmethod
    def _rollback(cls, state, children):
        pass

    def friendly_str(self):
        return '%s(%s)' % (self.__name__, ', '.join(['%s=%s' % (k, v) for k, v in self.state.iteritems()]))

    def friendly_html(self):
        return self.friendly_str()

    def run_threaded(self, abort_event):
        outer_handlers = get_thread_handlers()
        def thread_wrapped_task():
            with inner_thread_nested_setup(outer_handlers):
                try:
                    self.run()
                except Exception:
                    # TODO: Check Log
                    logbook.info(traceback.format_exc())
                    abort_event.set()
        thread = Thread(target=thread_wrapped_task, name=self.__name__)
        thread.start()
        return thread


class ExecTask(Task):
    desc_string = ''

    def _init(self, children, *args, **kwargs):
        self.save()

        for child in children:
            child.parent = self.id
            child.save()

    @classmethod
    def _run(cls, state, children):
        cls.execute(state, children)

    @classmethod
    def _rollback(cls, state, children):
        cls.execute(state, children)

    def friendly_str(self):
        return '%s%s' %\
                (type(self).desc_string, ''.join(['\n\t%s' % child.friendly_str() for child in self.children]))

    def friendly_html(self):
        return '%s<ul>%s</ul>' %\
                (type(self).desc_string,
                ''.join(['<li>%s</li>' % child.friendly_html() for child in self.children]))


class SequentialExecTask(ExecTask):
    def _init(self, children, *args, **kwargs):
        super(SequentialExecTask, self)._init(children, *args, **kwargs)
        self.state['task_order'] = [child.id for child in children]

    @staticmethod
    def execute(state, tasks):
        abort = Event()
        task_ids = {task.id: task for task in tasks}
        for task_id in state['task_order']:
            task = task_ids.pop(task_id)
            thread = task.run_threaded(abort)
            thread_wait(thread, abort)
        assert not task_ids, "SequentialExecTask has children that are not in\
                its task_order: %s" % (task_ids,)
        if abort.is_set():
            raise Exception('Caught exception while executing tasks sequentially')


class ParallelExecTask(ExecTask):
    desc_string = 'Execute in parallel:'

    @staticmethod
    def execute(state, tasks):
        threads = []
        abort = Event()
        for task in tasks:
            thread = task.run_threaded(abort)
            threads.append(thread)
        for thread in threads:
            thread_wait(thread, abort)
        if abort.is_set():
            raise Exception('Caught exception while executing tasks in parallel')


class DelayTask(Task):
    def _init(self, minutes, reversible=False):
        self.state.update(
                minutes=minutes,
                reversible=reversible)

    @classmethod
    def _run(cls, state, children):
        cls.wait(mins=state['minutes'])

    @classmethod
    def _rollback(cls, state, children):
        if state['reversible']:
            cls.wait(mins=state['minutes'])

    @staticmethod
    def wait(mins):
        logbook.info('Waiting for %sm' % (mins,),)
        sleep(mins*60)

    @classmethod
    def friendly_str(cls, minutes, **kwargs):
        return 'Delay for %s mins' % (minutes,)
