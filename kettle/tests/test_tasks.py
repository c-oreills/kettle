from datetime import datetime

from mock import patch, Mock

from kettle.db import session
from kettle.rollout import Rollout
from kettle.tasks import ReversibleTask
from kettle.tests import AlchemyTestCase

class TestReversibleTask(AlchemyTestCase):
    # ReversibleTask must be subclassed in order to save
    class BasicTask(ReversibleTask):
        pass

    def setUp(self):
        self.rollout = Rollout({})
        self.rollout.save()
        self.rollout_id = self.rollout.id

    @patch('kettle.tasks.ReversibleTask._init')
    def test_init(self, _init):
        task = self.BasicTask(self.rollout_id)
        self.assertEqual(task.rollout_id, self.rollout_id)
        self.assertTrue(self.BasicTask._init.called)

    def test_state(self):
        class StateTask(ReversibleTask):
            def _init(self, cake, pie):
                self.state['cake'] = cake
                self.state['pie'] = pie

        task = StateTask(self.rollout_id, 'a pie', 'a lie')
        task.save()

        session.Session.refresh(task)

        self.assertEqual(task.state['cake'], 'a pie')
        self.assertEqual(task.state['pie'], 'a lie')

    def test_run(self):
        # Datetime at 1 second resolution
        start = datetime.now().replace(microsecond=0)
        _run_mock = Mock(return_value=10)

        class RunTask(ReversibleTask):
            @classmethod
            def _run(cls, state):
                return _run_mock(cls, state)

        task = RunTask(self.rollout_id)

        self.assertEqual(task.run_start_dt, None)
        self.assertEqual(task.run_error, None)
        self.assertEqual(task.run_error_dt, None)
        self.assertEqual(task.run_return, None)
        self.assertEqual(task.run_return_dt, None)
        self.assertEqual(task.run_traceback, None)

        task.run()

        _run_mock.assert_called_once_with(RunTask, task.state)

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

        class RunTaskFail(ReversibleTask):
            @classmethod
            def _run(cls, state):
                return _run_mock(cls, state)

        task = RunTaskFail(self.rollout_id)

        self.assertEqual(task.run_start_dt, None)
        self.assertEqual(task.run_error, None)
        self.assertEqual(task.run_error_dt, None)
        self.assertEqual(task.run_return, None)
        self.assertEqual(task.run_return_dt, None)
        self.assertEqual(task.run_traceback, None)

        self.assertRaises(Exception, task.run)

        _run_mock.assert_called_once_with(RunTaskFail, task.state)

        self.assertGreaterEqual(task.run_start_dt, start)
        self.assertEqual(task.run_error, 'broken')
        self.assertGreaterEqual(task.run_error_dt, start)
        self.assertEqual(task.run_return, None)
        self.assertEqual(task.run_return_dt, None)
        self.assertIn('broken', task.run_traceback)

    def test_run_twice(self):
        task = self.BasicTask(self.rollout_id)

        task.run()
        self.assertRaises(Exception, task.run)

    def test_rollback_before_run(self):
        # Rolling back without running is valid
        _run_mock = Mock()

        class RunlessTask(ReversibleTask):
            @classmethod
            def _run(cls, state):
                return _run_mock(cls, state)

        task = RunlessTask(self.rollout_id)
        task.rollback()

        _run_mock.assert_not_called()
