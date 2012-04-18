from collections import defaultdict
from datetime import datetime
import time

from mock import patch, Mock

from kettle.db import session
from kettle.rollout import Rollout
from kettle.tasks import Task, DelayTask, SequentialExecTask, ParallelExecTask
from kettle.tests import KettleTestCase, TestTask, create_task

class TestTasks(KettleTestCase):
    def setUp(self):
        self.rollout = Rollout({})
        self.rollout.save()
        self.rollout_id = self.rollout.id

    @patch('kettle.tasks.Task._init')
    def test_init(self, _init):
        task = TestTask(self.rollout_id)
        self.assertEqual(task.rollout_id, self.rollout_id)
        self.assertTrue(TestTask._init.called)

    def test_state(self):
        class StateTask(Task):
            def _init(self, cake, pie):
                self.state['cake'] = cake
                self.state['pie'] = pie

        task = StateTask(self.rollout_id, 'a pie', 'a lie')
        task.save()

        session.Session.refresh(task)

        self.assertEqual(task.state['cake'], 'a pie')
        self.assertEqual(task.state['pie'], 'a lie')

    def test_no_rollout_id(self):
        task = TestTask(None)
        self.assertRaises(Exception, task.save)

    def test_run(self):
        # Datetime at 1 second resolution
        start = datetime.now().replace(microsecond=0)
        _run_mock = Mock(return_value=10)

        class RunTask(Task):
            @classmethod
            def _run(cls, state, children, abort, term):
                return _run_mock(cls, state, children, abort, term)

        task = RunTask(self.rollout_id)

        self.assertEqual(task.run_start_dt, None)
        self.assertEqual(task.run_error, None)
        self.assertEqual(task.run_error_dt, None)
        self.assertEqual(task.run_return, None)
        self.assertEqual(task.run_return_dt, None)
        self.assertEqual(task.run_traceback, None)

        task.run()

        _run_mock.assert_called_once_with(RunTask, task.state, task.children, None, None)

        self.assertGreaterEqual(task.run_start_dt, start)
        self.assertEqual(task.run_error, None)
        self.assertEqual(task.run_error_dt, None)
        self.assertEqual(task.run_return, str(10))
        self.assertGreaterEqual(task.run_return_dt, start)
        self.assertEqual(task.run_traceback, None)

    def test_run_fail(self):
        # Datetime at 1 second resolution
        start = datetime.now().replace(microsecond=0)
        _run_mock = Mock(return_value=10, side_effect=Exception('broken'))

        class RunTaskFail(Task):
            @classmethod
            def _run(cls, state, children, abort, term):
                return _run_mock(cls, state, children)

        task = RunTaskFail(self.rollout_id)

        self.assertEqual(task.run_start_dt, None)
        self.assertEqual(task.run_error, None)
        self.assertEqual(task.run_error_dt, None)
        self.assertEqual(task.run_return, None)
        self.assertEqual(task.run_return_dt, None)
        self.assertEqual(task.run_traceback, None)

        self.assertRaises(Exception, task.run)

        _run_mock.assert_called_once_with(RunTaskFail, task.state, task.children)

        self.assertGreaterEqual(task.run_start_dt, start)
        self.assertEqual(task.run_error, 'broken')
        self.assertGreaterEqual(task.run_error_dt, start)
        self.assertEqual(task.run_return, None)
        self.assertEqual(task.run_return_dt, None)
        self.assertIn('broken', task.run_traceback)

    def test_run_twice(self):
        task = TestTask(self.rollout_id)

        task.run()
        self.assertRaises(Exception, task.run)

    def test_revert_before_run(self):
        # Reverting without running is not valid
        _run_mock = Mock()

        class RunlessTask(Task):
            @classmethod
            def _run(cls, state, children, abort, term):
                return _run_mock(cls, state, children, abort, term)

        task = RunlessTask(self.rollout_id)
        self.assertRaises(Exception, task.revert)

        _run_mock.assert_not_called()

class TestSignals(KettleTestCase):
    def test_signals(self):
        rollouts = defaultdict(dict)
        rollout_ids = defaultdict(dict)
        tasks = defaultdict(dict)
        classes = DelayTask, SequentialExecTask, ParallelExecTask
        signals = ('abort_rollout',), ('term_rollout',), ('skip_rollback', 'term_rollout')
        for cls in classes:
            for sigs in signals:
                rollout = Rollout({})
                rollout.save()
                rollouts[cls][sigs] = rollout
                rollout_ids[cls][sigs] = rollout.id

                if cls is DelayTask:
                    task = create_task(rollout, DelayTask, seconds=15)
                    tasks[cls][sigs] = task
                else:
                    t1, t2, t3 = [create_task(rollout, DelayTask, seconds=15) for _ in range(3)]
                    task = create_task(rollout, cls, [t1, t2, t3])
                    tasks[cls][sigs] = t3

        for cls in classes:
            for sigs in signals:
                rollout = rollouts[cls][sigs]
                rollout.rollout_async()

        # Enough time for rollouts to start
        time.sleep(0.5)

        for cls in classes:
            for sigs in signals:
                id = rollout_ids[cls][sigs]
                for sig in sigs:
                    self.assertTrue(Rollout._can_signal(id, sig))
                    self.assertTrue(Rollout._do_signal(id, sig))
                    self.assertTrue(Rollout._is_signalling(id, sig))

        # Enough time for rollouts to finish and save to db
        time.sleep(2)

        for cls in classes:
            for sigs in signals:
                rollout_id = rollout_ids[cls][sigs]
                rollout = Rollout._from_id(rollout_id)
                print rollout, cls, sigs
                self.assertTrue(rollout.rollout_finish_dt, 'Rollout for %s not finished when sent %s' % (cls, sigs))
                task = tasks[cls][sigs]
                task = Task._from_id(task.id)
                # Sequential exec's last task should not have run due to aborts
                if cls is SequentialExecTask:
                    self.assertFalse(task.run_start_dt, 'Final task %s run for %s rollout when sent %s' % (task, cls, sigs))
                else:
                    self.assertTrue(task.run_start_dt, 'Final task %s not run for %s rollout when sent %s' % (task, cls, sigs))

                # If rollbacks were skipped the root task should not have reverted
                if 'skip_rollback' in sigs:
                    self.assertFalse(rollout.root_task.revert_start_dt, 'Rollout for %s rolled back when sent %s' % (cls, sigs))
                else:
                    self.assertTrue(rollout.root_task.revert_start_dt, 'Rollout for %s not rolled back when sent %s' % (cls, sigs))
