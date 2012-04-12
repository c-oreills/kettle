from mock import patch
from threading import Event

from kettle.rollout import Rollout
from kettle.tasks import SequentialExecTask, Task
from kettle.tests import AlchemyTestCase

calls = []

class TestTask(Task):
    def run(self):
        calls.append((self.id, 'run'))
        super(TestTask, self).run()

    def rollback(self):
        calls.append((self.id, 'rollback'))
        super(TestTask, self).rollback()


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
        if (task.id, 'run') not in calls:
            raise AssertionError('%s has not been run' % (task,))

    def assertNotRun(self, task):
        if (task.id, 'run') in calls:
            raise AssertionError('%s has been run' % (task,))

    def assertRollback(self, task):
        if (task.id, 'rollback') not in calls:
            raise AssertionError('%s has not been rolled back' % (task,))

    def assertNotRollback(self, task):
        if (task.id, 'rollback') in calls:
            raise AssertionError('%s has been rolled back' % (task,))

    def test_init(self):
        rollout = Rollout({})
        rollout.save()

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

        # Do not assert parents or root is run since otherwise we'd have to
        # override their call methods
        for task in fn1, fn2, fn3, fn4:
            self.assertRun(task)

    def test_quit_and_rollback_on_failure(self):
        class RecordedRollout(Rollout):
            rollback_calls = []

            def rollback(self):
                self.rollback_calls.append(self)
                super(RecordedRollout, self).rollback()

        rollout = RecordedRollout({})
        rollout.save()
        fn1 = create_task(rollout)
        fn_error = create_task(rollout, TestTaskFail)
        fn2 = create_task(rollout)
        root = create_task(rollout, SequentialExecTask, [fn1, fn_error, fn2])
        rollout.rollout()

        self.assertRun(fn1)
        self.assertRun(fn_error)
        self.assertNotRun(fn2)

        self.assertEqual(RecordedRollout.rollback_calls, [rollout])

        self.assertRollback(fn_error)
        self.assertRollback(fn1)
    
    def test_monitor_rolls_back(self):
        monitor_wake_event = Event()
        finish_step_event = Event()

        def monitor(monitor_event, abort_event):
            monitor_wake_event.wait()
            abort_event.set()
            finish_step_event.set()


        class MonitoredRollout(Rollout):
            monitors = {
                    'mon': monitor,
                    }
            rollback_calls = []

            def rollback(self):
                self.rollback_calls.append(self)
                super(MonitoredRollout, self).rollback()

        rollout = MonitoredRollout({'monitors': ['mon']})
        rollout.save()

        def wake_monitor_then_wait():
            monitor_wake_event.set()
            finish_step_event.wait()
            self.assertFalse(MonitoredRollout.rollback_calls)

        class WakeMonitorWaitTask(TestTask):
            @classmethod
            def _run(cls, state, children):
                wake_monitor_then_wait()

        fn_run = create_task(rollout)
        fn_wake_monitor_wait = create_task(rollout, WakeMonitorWaitTask)
        fn_not_called = create_task(rollout)
        root = create_task(rollout, SequentialExecTask, [fn_run, fn_wake_monitor_wait, fn_not_called])

        rollout.rollout()

        self.assertRun(fn_run)
        self.assertRun(fn_wake_monitor_wait)
        self.assertNotRun(fn_not_called)
        self.assertEqual(MonitoredRollout.rollback_calls, [rollout])
        self.assertRollback(fn_run)
        self.assertRollback(fn_wake_monitor_wait)
