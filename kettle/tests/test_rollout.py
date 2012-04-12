from threading import Event

from kettle.rollout import Rollout
from kettle.tasks import ParallelExecTask, SequentialExecTask, Task
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

    def test_single_task_rollout(self):
        rollout = Rollout({})
        rollout.save()
        root = create_task(rollout)

        rollout.rollout()

        self.assertRun(root)
        self.assertNotRollback(root)

    def test_single_task_rollback(self):
        rollout = Rollout({})
        rollout.save()
        root = create_task(rollout, TestTaskFail)

        rollout.rollout()

        self.assertRun(root)
        self.assertRollback(root)

    def _test_exec_rollout(self, exec_cls):
        rollout = Rollout({})
        rollout.save()
        task1 = create_task(rollout)
        task2 = create_task(rollout)
        task3 = create_task(rollout)
        task4 = create_task(rollout)
        parent1 = create_task(rollout, exec_cls, [task1, task2])
        parent2 = create_task(rollout, exec_cls, [task3, task4])
        root = create_task(rollout, exec_cls, [parent1, parent2])

        rollout.rollout()

        # Do not assert parents or root is run since otherwise we'd have to
        # override their call methods
        for task in task1, task2, task3, task4:
            self.assertRun(task)
            self.assertNotRollback(task)

    def test_sequential_exec_rollout(self):
        self._test_exec_rollout(SequentialExecTask)

    def test_parallel_exec_rollout(self):
        self._test_exec_rollout(ParallelExecTask)

    def test_sequential_quit_and_rollback_on_failure(self):
        class RecordedRollout(Rollout):
            rollback_calls = []

            def rollback(self):
                self.rollback_calls.append(self)
                super(RecordedRollout, self).rollback()

        rollout = RecordedRollout({})
        rollout.save()
        task1 = create_task(rollout)
        task_error = create_task(rollout, TestTaskFail)
        task2 = create_task(rollout)
        root = create_task(rollout, SequentialExecTask, [task1, task_error, task2])
        rollout.rollout()

        self.assertRun(task1)
        self.assertRun(task_error)
        self.assertNotRun(task2)

        self.assertEqual(RecordedRollout.rollback_calls, [rollout])

        self.assertRollback(task_error)
        self.assertRollback(task1)
        self.assertNotRollback(task2)
    
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

        task_run = create_task(rollout)
        task_wake_monitor_wait = create_task(rollout, WakeMonitorWaitTask)
        task_not_called = create_task(rollout)
        root = create_task(rollout, SequentialExecTask, [task_run, task_wake_monitor_wait, task_not_called])

        rollout.rollout()

        self.assertRun(task_run)
        self.assertRun(task_wake_monitor_wait)
        self.assertNotRun(task_not_called)
        self.assertEqual(MonitoredRollout.rollback_calls, [rollout])
        self.assertRollback(task_run)
        self.assertRollback(task_wake_monitor_wait)
