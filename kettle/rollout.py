from datetime import datetime
from os import path
from threading import Event, Thread

from sqlalchemy import Column, DateTime, Integer, PickleType, Boolean, orm
from logbook import FileHandler, NestedSetup, NullHandler

from db import Base, session
from db.fields import JSONEncodedDict
import settings
from tasks import thread_wait

class Rollout(Base):
    __tablename__ = 'rollout'
    id = Column(Integer, primary_key=True)
    config = Column(JSONEncodedDict(1000))
    stages = Column(PickleType)

    hidden = Column(Boolean, default=False)

    generate_tasks = Column(DateTime)

    rollout_start_dt = Column(DateTime)
    rollout_finish_dt = Column(DateTime)

    rollback_start_dt = Column(DateTime)
    rollback_finish_dt = Column(DateTime)
    
    monitors = {}
    abort_signals = {}

    def __init__(self, config):
        self.config = config

    def rollout(self):
        if not self.tasks:
            raise Exception('No tasks to rollout')

        if self.rollout_start_dt:
            raise Exception('Rollout already started at %s' % 
                    (self.rollout_start_dt))

        self.aborting = Event()
        self.monitoring = Event()
        self.rollout_start_dt = datetime.now()
        self.save()
        self.abort_signals[self.id] = self.aborting

        self.root_task # Check root task exists before starting

        self.start_monitoring()
        try:
            with self.log_setup_rollout():
                task_thread = self.root_task.run_threaded(self.aborting)
                thread_wait(task_thread, self.aborting)
            if self.aborting.is_set():
                if not self.rollout_finish_dt:
                    self.rollout_finish_dt = datetime.now()
                    self.save()
                self.rollback()
        finally:
            self.stop_monitoring()
            if not self.rollout_finish_dt:
                self.rollout_finish_dt = datetime.now()
            self.abort_signals.pop(self.id)
            self.save()

    @property
    def root_task(self):
        try:
            root_task = session.Session.query('Task').filter_by(
                    rollout=self, parent=None).one()
        except orm.exc.MultipleResultsFound:
            raise Exception('Could not get root task: more than one task has no \
                    parents: %s' % (self.tasks.filter_by(parent=None).all()))
        except orm.exc.NoResultFound:
            raise Exception('Could not get root task: no tasks have no parents')
        else:
            return root_task

    def rollout_async(self):
        # remove stops error caused by having rollout in multiple sessions
        session.Session.remove()
        rollout_thread = Thread(target=self.rollout)
        rollout_thread.start()

    def start_monitoring(self):
        if self.monitoring.is_set():
            return 

        self.monitoring.set()

        monitors = [v for k, v in self.monitors.iteritems() 
                if k in self.config.get('monitors', [])]
        for monitor in monitors:
            thread = Thread(
                    target=monitor, args=(self.monitoring, self.aborting),
                    name='monitor: %s' % monitor.__name__)
            thread.daemon = True
            thread.start()

    def stop_monitoring(self):
        self.monitoring.clear()

    def rollback(self):
        self.rollback_start_dt = datetime.now()
        self.save()

        with self.log_setup_rollback():
            self.root_task.rollback()

        self.rollback_finish_dt = datetime.now()
        self.save()

    def save(self):
        if self not in session.Session:
            session.Session.add(self)
        session.Session.commit()

    def generate_tasks(self):
        self._generate_tasks()
        self.generate_tasks_dt = datetime.now()

    def _generate_tasks(self):
        pass # Override

    def nice_status(self):
        if not self.rollout_start_dt:
            return 'Not started'
        else:
            if not self.rollback_start_dt:
                if not self.rollout_finish_dt:
                    return 'Started at %s' % self.rollout_start_dt
                else:
                    return 'Finished'
            else:
                if not self.rollback_finish_dt:
                    return 'Rolling back at %s' % self.rollback_start_dt
                else:
                    return 'Rolled back'

    @classmethod
    def abort(cls, id):
        try:
            cls.abort_signals[id].set()
        except KeyError:
            raise Exception('No abort signal found for rollout_id %s' % (id,))

    @property
    def can_abort(self):
        abort_signal = Rollout.abort_signals.get(self.id)
        return abort_signal and not abort_signal.is_set()

    @property
    def info_list(self):
        "A list of HTML strings to be displayed in bullet points in the rollout view"
        pass

    def log_filename(self, *args):
        return path.join(settings.LOG_DIR, '.'.join(map(str, (self.id,) + args)))

    base_handlers = (NullHandler(),)

    def log_setup_rollout(self):
        return NestedSetup(self.base_handlers + (
                FileHandler(self.log_filename('rollout'), bubble=True),
                FileHandler(self.log_filename(
                    self.current_stage, self.current_step), bubble=True)))

    def log_setup_rollback(self):
        return NestedSetup(
                self.base_handlers + (
                    FileHandler(self.log_filename('rollback'), bubble=True),))
