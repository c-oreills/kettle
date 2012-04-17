from collections import defaultdict
from datetime import datetime
from threading import Event, Thread

from sqlalchemy import Column, DateTime, Integer, PickleType, Boolean, orm
from logbook import FileHandler, NestedSetup, NullHandler

from db import Base, session
from db.fields import JSONEncodedDict
from log_utils import log_filename
from tasks import thread_wait

ROLLOUT_SIGNALS = ('abort_rollout', 'term_rollout', 'monitoring', 'skip_rollback')
ROLLBACK_SIGNALS = ('abort_rollback', 'term_rollback')
ALL_SIGNALS = ROLLOUT_SIGNALS + ROLLBACK_SIGNALS

class Rollout(Base):
    __tablename__ = 'rollout'
    id = Column(Integer, primary_key=True)
    config = Column(JSONEncodedDict(1000))
    stages = Column(PickleType)

    hidden = Column(Boolean, default=False)

    generate_tasks_dt = Column(DateTime)

    rollout_start_dt = Column(DateTime)
    rollout_finish_dt = Column(DateTime)

    rollback_start_dt = Column(DateTime)
    rollback_finish_dt = Column(DateTime)
    
    monitors = {}
    signals = defaultdict(dict)

    def __init__(self, config):
        self.config = config

    @classmethod
    def _from_id(cls, id):
        return session.Session.query(cls).get(id)

    def rollout(self):
        self._rollout(self.id)

    @classmethod
    def _rollout(cls, id):
        self = cls._from_id(id)
        if not self.root_task:
            raise Exception('No root task to rollout')

        if self.rollout_start_dt:
            raise Exception('Rollout already started at %s' % 
                    (self.rollout_start_dt))

        self.rollout_start_dt = datetime.now()
        self.save()
        self._setup_signals_rollout()

        self.root_task # Check root task exists before starting

        self.start_monitoring()
        try:
            with self.log_setup_rollout():
                abort_rollout = self.signal('abort_rollout')
                task_thread = self.root_task.run_threaded(abort_rollout)
                thread_wait(task_thread, abort_rollout)
            failed = self.is_aborting('rollout') or self.is_terming('rollout')
            if failed and not self.is_skipping('rollback'):
                self._update_rollout_finish_dt()
                self.rollback()
        finally:
            self.stop_monitoring()
            self._update_rollout_finish_dt()
            self._teardown_signals_rollout()

    def rollback(self):
        self.rollback_start_dt = datetime.now()
        self.save()
        self._setup_signals_rollback()

        with self.log_setup_rollback():
            self.root_task.revert()

        self.rollback_finish_dt = datetime.now()
        self.save()
        self._teardown_signals_rollback()

    @property
    def root_task(self):
        from kettle.tasks import Task
        try:
            root_task = session.Session.query(Task).filter(
                    Task.rollout==self, Task.parent==None).one()
        except orm.exc.MultipleResultsFound:
            raise Exception('Could not get root task: more than one task has no \
                    parents: %s' % (self.tasks.filter_by(parent=None).all()))
        except orm.exc.NoResultFound:
            raise Exception('Could not get root task: no tasks have no parents')
        else:
            return root_task

    def rollout_async(self):
        rollout_id = self.id
        # expunge stops error caused by having rollout in multiple sessions
        session.Session.expunge(self)
        rollout_thread = Thread(target=self._rollout, args=(rollout_id,))
        rollout_thread.start()
        return rollout_thread

    def start_monitoring(self):
        monitoring = self.signal('monitoring')
        if monitoring.is_set():
            return 

        monitoring.set()

        monitors = [v for k, v in self.monitors.iteritems() 
                if k in self.config.get('monitors', [])]
        for monitor in monitors:
            thread = Thread(
                    target=monitor, args=(monitoring, self.signal('abort_rollout')),
                    name='monitor: %s' % monitor.__name__)
            thread.daemon = True
            thread.start()

    def stop_monitoring(self):
        monitoring = self.signal('monitoring')
        monitoring.clear()

    def save(self):
        if self not in session.Session:
            session.Session.add(self)
        session.Session.commit()

    def generate_tasks(self):
        if self.rollout_start_dt:
            raise Exception('Cannot generate tasks after rollout has started')
        # Clear any previous tasks
        map(session.Session.delete, self.tasks)
        session.Session.commit()
        self._generate_tasks()
        self.generate_tasks_dt = datetime.now()

    def _generate_tasks(self):
        pass # Override

    def status(self):
        if not self.rollout_start_dt:
            return 'not_started'

        if not self.rollback_start_dt:
            if self.is_terming('rollout'):
                return 'terminating_rollout'
            if self.is_aborting('rollout'):
                return 'aborting_rollout'
            if not self.rollout_finish_dt:
                return 'started'
            else:
                return 'finished'
        else:
            if self.is_terming('rollback'):
                return 'terminating_rollback'
            if self.is_aborting('rollback'):
                return 'aborting_rollback'
            if not self.rollback_finish_dt:
                return 'rolling_back'
            else:
                return 'rolled_back'

    def friendly_status(self):
        status = self.status()
        return {
                'started': 'Started at %s' % self.rollout_start_dt,
                'rolling_back': 'Rolling back at %s' % self.rollback_start_dt,
                }.get(status, status.title().replace('_', ' '))

    def friendly_status_html(self):
        return '<span id="rollout_{id}_status" class="status {status}">{friendly_status}</span>'.format(
                id=self.id, status=self.status(), friendly_status=self.friendly_status())

    def rollout_friendly_status(self):
        return self.exec_friendly_status('rollout')

    def rollback_friendly_status(self):
        return self.exec_friendly_status('rollback')

    def exec_friendly_status(self, action):
        start_dt = getattr(self, '%s_start_dt' % action)
        finish_dt = getattr(self, '%s_finish_dt' % action)
        if start_dt:
            if not finish_dt:
                return 'Started at %s' % start_dt
            else:
                return '%s - %s' % (start_dt, finish_dt)
        else:
            if not finish_dt:
                return 'Not started'
            else:
                return 'Error: no start time, finished %s' % finish_dt

    @staticmethod
    def _check_signal_name(signal_name):
        if signal_name not in ALL_SIGNALS:
            raise Exception('Invalid signal name: %s' % signal_name)

    @classmethod
    def get_signal(cls, id, signal_name):
        cls._check_signal_name(signal_name)
        return cls.signals[id].get(signal_name)

    def signal(self, signal_name):
        return self.get_signal(self.id, signal_name)

    def _make_signal(self, signal_name):
        self._check_signal_name(signal_name)
        self.signals[self.id][signal_name] = Event()

    def _del_signal(self, signal_name):
        self._check_signal_name(signal_name)
        del self.signals[self.id][signal_name]

    def abort(self, action):
        return self._do_signal(self.id, 'abort_%s' % action)

    def can_abort(self, action):
        return self._can_signal(self.id, 'abort_%s' % action)

    def is_aborting(self, action):
        return self._is_signalling(self.id, 'abort_%s' % action)

    def term(self, action):
        return self._do_signal(self.id, 'term_%s' % action)

    def can_term(self, action):
        return self._can_signal(self.id, 'term_%s' % action)

    def is_terming(self, action):
        return self._is_signalling(self.id, 'term_%s' % action)

    def skip(self, action):
        return self._do_signal(self.id, 'skip_%s' % action)

    def can_skip(self, action):
        return self._can_signal(self.id, 'skip_%s' % action)

    def is_skipping(self, action):
        return self._is_signalling(self.id, 'skip_%s' % action)

    @classmethod
    def _do_signal(cls, id, signal_name):
        if not cls._can_signal(id, signal_name):
            return False
        signal = cls.get_signal(id, signal_name)
        signal.set()
        return True

    @classmethod
    def _can_signal(cls, id, signal_name):
        signal = cls.get_signal(id, signal_name)
        return signal and not signal.is_set()

    @classmethod
    def _is_signalling(cls, id, signal_name):
        signal = cls.get_signal(id, signal_name)
        return signal and signal.is_set()

    def _setup_signals_rollout(self):
        map(self._make_signal, ROLLOUT_SIGNALS)

    def _teardown_signals_rollout(self):
        map(self._del_signal, ROLLOUT_SIGNALS)

    def _setup_signals_rollback(self):
        map(self._make_signal, ROLLBACK_SIGNALS)

    def _teardown_signals_rollback(self):
        map(self._del_signal, ROLLBACK_SIGNALS)

    def _update_rollout_finish_dt(self):
        if not self.rollout_finish_dt:
            self.rollout_finish_dt = datetime.now()
            self.save()

    @property
    def info_list(self):
        "A list of HTML strings to be displayed in bullet points in the rollout view"
        pass

    base_handlers = (NullHandler(),)

    def log_setup_rollout(self):
        return self.log_setup_generic('rollout')

    def log_setup_rollback(self):
        return self.log_setup_generic('rollback')

    def log_setup_generic(self, action):
        return NestedSetup(
                self.base_handlers + (
                    FileHandler(log_filename(self.id, action), bubble=True),))
