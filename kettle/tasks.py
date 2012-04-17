from datetime import datetime
from time import sleep
import traceback

import logbook
from logbook import FileHandler, NestedSetup
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship, backref

from db import Base, session
from db.fields import JSONEncodedDict
from log_utils import log_filename, get_thread_handlers
from thread_utils import make_exec_threaded, thread_wait

def action_fn(action):
    def do_action(instance):
        action_start_dt = getattr(instance, '%s_start_dt' % (action,), None)
        if not action_start_dt:
            instance.call_and_record_action(action)
        else:
            raise Exception('%s already started at %s' % 
                    (action, action_start_dt))
    return do_action


class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    type = Column(String(50), nullable=False)
    rollout_id = Column(Integer, ForeignKey('rollout.id'), nullable=False)
    parent_id = Column(Integer, ForeignKey('task.id'))
    state = Column(JSONEncodedDict(1000))

    run_start_dt = Column(DateTime)
    run_error = Column(String(500))
    run_error_dt = Column(DateTime)
    run_return = Column(String(500))
    run_return_dt = Column(DateTime)
    run_traceback = Column(String(1000))

    revert_start_dt = Column(DateTime)
    revert_error = Column(String(500))
    revert_error_dt = Column(DateTime)
    revert_return = Column(String(500))
    revert_return_dt = Column(DateTime)
    revert_traceback = Column(String(1000))

    rollout = relationship('Rollout', backref=backref('tasks', order_by=id))
    children = relationship('Task', backref=backref('parent', remote_side='Task.id', order_by=id))


    @declared_attr
    def __mapper_args__(cls):
        # Implementing single table inheritance
        if cls.__name__ == 'Task':
            return {'polymorphic_on': cls.type,}
        else:
            return {'polymorphic_identity': cls.__name__}

    def __init__(self, rollout_id, *args, **kwargs):
        self.rollout_id = rollout_id
        self.state = {}
        self._init(*args, **kwargs)

    @classmethod
    def _from_id(cls, id):
        return session.Session.query(cls).get(id)

    def _init(self, *args, **kwargs):
        pass

    @classmethod
    def _run(cls, state, children, abort, term):
        pass

    @classmethod
    def _revert(cls, state, children, abort, term):
        pass

    run = action_fn('run')
    __revert = action_fn('revert')

    def revert(self):
        if not self.run_start_dt:
            raise Exception('Cannot revert before running')
        self.__revert()

    run_threaded = make_exec_threaded('run')
    revert_threaded = make_exec_threaded('revert')

    def get_signals(self, action):
        from rollout import Rollout
        signal_action = {'run': 'rollout', 'revert': 'rollback'}[action]
        abort_signal = Rollout.get_signal(self.rollout_id, 'abort_%s' % signal_action)
        term_signal = Rollout.get_signal(self.rollout_id, 'term_%s' % signal_action)
        if not abort_signal and term_signal:
            raise Exception('Cannot run: one or more signals don\'t exist: '
                    'abort: %s, term: %s' % (abort_signal, term_signal))
        return abort_signal, term_signal

    def call_and_record_action(self, action):
        setattr(self, '%s_start_dt' % (action,), datetime.now())
        self.save()
        # action_fn should be a classmethod, hence getattr explicitly on type
        action_fn = getattr(type(self), '_%s' % (action,))
        try:
            with self.log_setup_action(action):
                abort, term = self.get_signals(action)
                action_return = action_fn(self.state, self.children, abort, term)
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

    def log_setup_action(self, action):
        return NestedSetup(
                get_thread_handlers() +
                (FileHandler(log_filename(
                    self.rollout_id, self.id, action), bubble=True),))

    def save(self):
        if self not in session.Session:
            session.Session.add(self)
        session.Session.commit()

    def __repr__(self):
        return '<%s: id=%s, rollout_id=%s, state=%s>' % (
                self.__class__.__name__, self.id, self.rollout_id, repr(self.state))

    def friendly_str(self):
        return repr(self)

    def friendly_html(self):
        from kettleweb.app import url_for
        inner = [self.friendly_str()]
        for action in 'run', 'revert':
            if not getattr(self, '%s_start_dt' % action):
                continue
            log_url = url_for(
                    'log_view', rollout_id=self.rollout_id, args='/'.join(
                        map(str, (self.id, action))))
            log_link = '<a href="{url}">{action}</a>'.format(
                    url=log_url, action='%s log' % action.title())
            inner.append(log_link)
        return '<span class="task {class_}">{inner}</span>'.format(
                class_=self.status(), inner=' '.join(inner))

    def status(self):
        if not self.run_start_dt:
            return 'not_started'
        else:
            if not self.revert_start_dt:
                if not self.run_return_dt:
                    return 'started'
                else:
                    return 'finished'
            else:
                if not self.revert_return_dt:
                    return 'rolling_back'
                else:
                    return 'rolled_back'


class ExecTask(Task):
    desc_string = ''

    def _init(self, children, *args, **kwargs):
        self.save()

        for child in children:
            child.parent = self
            child.save()

    @classmethod
    def _run(cls, state, children, abort, term):
        cls.exec_forwards(state, children, abort, term)

    @classmethod
    def _revert(cls, state, children, abort, term):
        cls.exec_backwards(state, children, abort, term)

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

    @classmethod
    def exec_forwards(cls, state, tasks, abort, term):
        cls.exec_tasks('run_threaded', state['task_order'], tasks, abort, term)

    @classmethod
    def exec_backwards(cls, state, tasks, abort, term):
        run_tasks = [t for t in tasks if t.run_start_dt]
        run_task_ids = set(t.id for t in run_tasks)
        task_order = [t_id for t_id in reversed(state['task_order']) if t_id in run_task_ids]
        cls.exec_tasks('revert_threaded', task_order, run_tasks, abort, term)

    @staticmethod
    def exec_tasks(method_name, task_order, tasks, abort, term):
        task_ids = {task.id: task for task in tasks}
        for task_id in task_order:
            if abort.is_set() or term.is_set():
                break
            task = task_ids.pop(task_id)
            thread = getattr(task, method_name)(abort)
            thread_wait(thread, abort)
            if thread.exc_info is not None:
                raise Exception('Caught exception while executing task %s: %s' %
                        (task, thread.exc_info))
        else:
            assert not task_ids, ("SequentialExecTask has children that are not "
                    "in its task_order: %s" % (task_ids,))

    def friendly_html(self):
        task_order = self.state['task_order']
        child_ids = {c.id: c for c in self.children}
        return '%s<ul>%s</ul>' %\
                (type(self).desc_string,
                ''.join(['<li>%s</li>' % child_ids[id].friendly_html() for id in task_order]))


class ParallelExecTask(ExecTask):
    desc_string = 'Execute in parallel:'

    @classmethod
    def exec_forwards(cls, state, tasks, abort, term):
        cls.exec_tasks('run_threaded', tasks, abort, term)

    @classmethod
    def exec_backwards(cls, state, tasks, abort, term):
        cls.exec_tasks('revert_threaded', [t for t in tasks if t.run_start_dt],
                abort, term)

    @staticmethod
    def exec_tasks(method_name, tasks, abort, term):
        threads = []
        for task in tasks:
            if abort.is_set() or term.is_set():
                break
            thread = getattr(task, method_name)(abort)
            threads.append(thread)
        for thread in threads:
            thread_wait(thread, abort)
            if thread.exc_info is not None:
                raise Exception('Caught exception while executing task %s: %s' %
                        (thread.target, thread.exc_info))


class DelayTask(Task):
    def _init(self, minutes=0, seconds=0, reversible=False):
        self.state.update(
                minutes=minutes,
                seconds=seconds,
                reversible=reversible)

    @classmethod
    def _run(cls, state, children, abort, term):
        cls.wait(cls.get_secs(state), abort=abort, term=term)

    @classmethod
    def _revert(cls, state, children, abort, term):
        if state['reversible']:
            cls.wait(cls.get_secs(state), abort=abort, term=term)

    @classmethod
    def wait(cls, secs, abort, term):
        logbook.info('Waiting for %s' % (cls.min_sec_str(secs),),)
        for i in xrange(int(secs)):
            if abort.is_set() or term.is_set():
                break
            sleep(1)

    def friendly_str(self):
        if self.run_start_dt and not self.run_return_dt:
            remaining_secs = self.state['minutes']*60 - (datetime.now() - self.run_start_dt).seconds
            ' (%s left)' % self.min_sec_str(remaining_secs)
        else:
            remaining_str = ''

        rev_str = ' (reversible)' if self.state['reversible'] else ''
        return 'Delay for {minutes} mins{remaining_str}{rev_str}'.format(
                remaining_str=remaining_str, rev_str=rev_str, **self.state)

    @staticmethod
    def min_sec_str(secs):
        mins = secs / 60
        secs = secs % 60
        if mins:
            return '%d:%02d mins' % (mins, secs)
        else:
            return '%d secs' % (secs,)

    @staticmethod
    def get_secs(state):
        return (state.get('minutes', 0) * 60) + state.get('seconds', 0)
