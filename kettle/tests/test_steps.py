from unittest import TestCase

from mock import Mock

from kettle.steps import parse_step

class TestSteps(TestCase):
    def test_parse_step(self):
        fn = Mock()

        task, args, kwargs = parse_step((fn,))
        self.assertEqual(task, fn)
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {})

        task, args, kwargs = parse_step((fn, ('cake',)))
        self.assertEqual(task, fn)
        self.assertEqual(args, ('cake',))
        self.assertEqual(kwargs, {})

        task, args, kwargs = parse_step((fn, {'pie': 12}))
        self.assertEqual(task, fn)
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {'pie': 12})

        task, args, kwargs = parse_step((fn, ('cake',), {'pie': 12}))
        self.assertEqual(task, fn)
        self.assertEqual(args, ('cake',))
        self.assertEqual(kwargs, {'pie': 12})
