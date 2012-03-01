from datetime import datetime
from os import path
from threading import Event, Thread

from sqlalchemy import Column, DateTime, Integer, PickleType, Boolean
from logbook import FileHandler, NestedSetup, NullHandler

from db import Base, session
from db.fields import JSONEncodedDict
import settings
from steps import thread_wait, run_step_thread

class Rollout(Base):
    __tablename__ = 'rollout'
    id = Column(Integer, primary_key=True)
    config = Column(JSONEncodedDict(1000))
    stages = Column(PickleType)

    hidden = Column(Boolean, default=False)

    generate_stages_dt = Column(DateTime)

    rollout_start_dt = Column(DateTime)
    rollout_finish_dt = Column(DateTime)

    rollback_start_dt = Column(DateTime)
    rollback_finish_dt = Column(DateTime)
    
    # Purely for nice display in web client
    current_stage = Column(Integer)
    current_step = Column(Integer)

    monitors = {}
    abort_signals = {}

    def __init__(self, config):
        self.config = config
        self.stages = []

    def rollout(self):
        if not self.stages:
            raise Exception('No stages to rollout')

        if self.rollout_start_dt:
            raise Exception('Rollout already started at %s' % 
                    (self.rollout_start_dt))

        self.aborting = Event()
        self.monitoring = Event()
        self.rollout_start_dt = datetime.now()
        self.save()
        self.abort_signals[self.id] = self.aborting

        self.start_monitoring()
        try:
            for stage_num, stage in enumerate(self.stages):
                if self.aborting.is_set():
                    break
                for step_num, step in enumerate(stage['steps']):
                    if self.aborting.is_set():
                        break
                    self.current_stage, self.current_step = stage_num, step_num
                    self.save()
                    with self.log_setup_rollout():
                        self._step_loop(step)
        finally:
            self.stop_monitoring()
            if not self.rollout_finish_dt:
                self.rollout_finish_dt = datetime.now()
            self.abort_signals.pop(self.id)
            self.save()

    def rollout_async(self):
        # remove stops error caused by having rollout in multiple sessions
        session.Session.remove()
        rollout_thread = Thread(target=self.rollout)
        rollout_thread.start()

    def _step_loop(self, step):
        step_thread = run_step_thread(self.id, step, self.aborting)
        thread_wait(step_thread, self.aborting)
        if self.aborting.is_set():
            if not self.rollout_finish_dt:
                self.rollout_finish_dt = datetime.now()
                self.save()
            self.rollback()

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
        # TODO: Should we explicitly refresh from db here?
        print '****** Rollback ******'
        self.rollback_start_dt = datetime.now()
        self.save()

        task_rollback_order = sorted(
                self.tasks, key=lambda t: (t.run_start_dt, t.id), reverse=True)
        with self.log_setup_rollback():
            for task in task_rollback_order:
                task.rollback()

        self.rollback_finish_dt = datetime.now()
        self.save()

    def save(self):
        if self not in session.Session:
            session.Session.add(self)
        session.Session.commit()

    def generate_stages(self):
        self._generate_stages()
        self.generate_stages_dt = datetime.now()

    def _generate_stages(self):
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
                    FileHandler(self.log_filename('rollout'), bubble=True)))
