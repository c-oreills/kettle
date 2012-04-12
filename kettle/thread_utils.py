from threading import Thread
import sys
import traceback

import logbook

from db import session
from log_utils import get_thread_handlers, inner_thread_nested_setup

class FailingThread(Thread):
    def __init__(self, *args, **kwargs):
        super(FailingThread, self).__init__(*args, **kwargs)
        self.exc_info = None

    def run(self):
        try:
            super(FailingThread, self).run()
        except Exception:
            self.exc_info = sys.exc_info()


def make_exec_threaded(method_name):
    def _exec_threaded(instance, abort_event):
        outer_handlers = get_thread_handlers()
        task_id = instance.id
        def thread_wrapped_task():
            with inner_thread_nested_setup(outer_handlers):
                try:
                    # Reload from db
                    task = session.Session.query('Task').filter_by(id=task_id).one()
                    getattr(task, method_name)()
                except Exception:
                    # TODO: Fix logging
                    print traceback.format_exc()
                    logbook.exception()
                    abort_event.set()
        thread = FailingThread(target=thread_wrapped_task, name=instance.__class__.__name__)
        thread.start()
        return thread
    return _exec_threaded

def thread_wait(thread, abort_event):
    try:
        while True:
            if abort_event.is_set():
                pass # TODO: Set some kinda timeout
            if thread.is_alive():
                thread.join(1)
            else:
                break
    except Exception:
        # TODO: Fix logging
        print traceback.format_exc()
        logbook.exception()
        abort_event.set()
