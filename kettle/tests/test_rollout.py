from mock import patch
from threading import Event

from kettle.rollout import Rollout
from kettle.tasks import SequentialExecTask, Task
from kettle.tests import AlchemyTestCase

calls = []

class TestTask(Task):
    def run(self):
        calls.append((self.id, 'run'))

    def rollback(self):
        calls.append((self.id, 'rollback'))


class TestTaskFail(TestTask):
    @classmethod
    def _run(cls, state, children):
        raise Exception


def create_task(rollout, task_cls=None, *args):
    if task_cls is None:
        task_cls = TestTask
    task = task_cls(rollout.id, *args)
    task.save()
    return task


class TestRollout(AlchemyTestCase):
    def tearDown(self):
        del calls[:]

    def assertRun(self, task):
        self.assertIn((task.id, 'run'), calls)

    def assertNotRun(self, task):
        self.assertNotIn((task.id, 'run'), calls)

    def assertRollback(self, task):
        self.assertIn((task.id, 'rollback'), calls)

    def assertNotRollback(self, task):
        self.assertNotIn((task.id, 'rollback'), calls)

    def test_init(self):
        Rollout({})

    def test_rollout(self):
        rollout = Rollout({})
        rollout.save()
        fn1 = create_task(rollout)
        fn2 = create_task(rollout)
        fn3 = create_task(rollout)
        fn4 = create_task(rollout)
        parent1 = create_task(rollout, SequentialExecTask, [fn1, fn2])
        parent2 = create_task(rollout, SequentialExecTask, [fn3, fn4])
        root = create_task(rollout, SequentialExecTask, [parent1, parent2])

        rollout.rollout()

        for task in fn1, fn2, fn3, fn4, parent1, parent2, root:
            self.assertRun(task)

    @patch('kettle.rollout.Rollout.rollback')
    def test_quit_and_rollback_on_failure(self, rollback):
        rollout = Rollout({})
        fn1 = create_task(rollout)
        fn_error = create_task(rollout, TestTaskFail)
        fn2 = create_task(rollout)
        root = create_task(rollout, SequentialExecTask, [fn1, fn_error, fn2])
        rollout.rollout()

        self.assertRun(fn1)
        self.assertRun(fn_error)
        self.assertNotRun(fn2)

        self.assertTrue(rollback.called)

        self.assertRollback(fn_error)
        self.assertRollback(fn1)
    
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
