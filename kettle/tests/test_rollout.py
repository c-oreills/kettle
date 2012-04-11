from mock import patch
from threading import Event

from kettle.rollout import Rollout
from kettle.tests import AlchemyTestCase

from mock_functions import calls, clear_calls, fn1, fn2, fn3, fn_error

class TestRollout(AlchemyTestCase):
    def tearDown(self):
        clear_calls()

    def test_init(self):
        Rollout({})

    def test_rollout(self):
        rollout = Rollout({})
        rollout.stages = [
                make_stage('stage1', [(fn1,), (fn1,)]),
                make_stage('stage2', [(fn2,), (fn3,)])
                ]
        rollout.rollout()

        called_fns = [fn for fn, _, _ in calls]

        self.assertIn(fn1, called_fns)
        self.assertIn(fn2, called_fns)

    @patch('kettle.rollout.Rollout.rollback')
    def test_quit_and_rollback_on_failure(self, rollback):
        rollout = Rollout({})
        rollout.stages = [
                make_stage('stage1', [(fn1,)]),
                make_stage('stage2', [(fn_error,)]),
                make_stage('stage3', [(fn2,)])
                ]
        rollout.rollout()

        called_fns = [fn for fn, _, _ in calls]

        self.assertIn(fn1, called_fns)
        self.assertIn(fn_error, called_fns)
        self.assertNotIn(fn2, called_fns)

        self.assertTrue(rollback.called)
    
    @patch('kettle.rollout.Rollout.save') # Inline functions break pickle
    @patch('kettle.rollout.Rollout.rollback')
    def test_monitor_rolls_back(self, rollback, save):
        monitor_wake_event = Event()
        finish_step_event = Event()

        def monitor(monitor_event, abort_event):
            monitor_wake_event.wait()
            abort_event.set()
            finish_step_event.set()

        def wake_monitor_then_wait(*args, **kwargs):
            monitor_wake_event.set()
            finish_step_event.wait()
            self.assertFalse(rollback.called)

        class MonitoredRollout(Rollout):
            monitors = {
                    'mon': monitor,
                    }

        rollout = MonitoredRollout({'monitors': ['mon']})
        rollout.stages = [
                make_stage('stage_first', [(fn1,)]),
                make_stage('stage_wait', [(wake_monitor_then_wait,)]),
                make_stage('stage_not_called', [(fn2,)])
                ]

        rollout.rollout()

        called_fns = [fn for fn, _, _ in calls]

        self.assertNotIn(fn2, called_fns)
        self.assertTrue(rollback.called)
