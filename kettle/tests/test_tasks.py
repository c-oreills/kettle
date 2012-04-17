from datetime import datetime

from mock import patch, Mock

from kettle.db import session
from kettle.rollout import Rollout
from kettle.tasks import Task, DelayTask
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

class TestDelayTask(KettleTestCase):
    def setUp(self):
        self.rollout = Rollout({})
        self.rollout.save()
        self.rollout_id = self.rollout.id

    def test_abort_fast(self):
        create_task(self.rollout, DelayTask, seconds=2)
        #create_task(self.rollout)
        self.rollout.rollout_async()
        self.assertTrue(self.rollout.abort('rollout'))
        self.assertTrue(self.rollout.signal('abort_rollout').is_set())
        self.rollout = Rollout._from_id(self.rollout_id)
        self.assertTrue(self.rollout.rollout_finish_dt)
