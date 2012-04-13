from threading import Thread
import sys
import traceback

import logbook

from log_utils import get_thread_handlers, inner_thread_nested_setup

class ExcRecordingThread(Thread):
    "Thread that catches exceptions and stores them to its exc_info attribute"
    def __init__(self, *args, **kwargs):
        super(ExcRecordingThread, self).__init__(*args, **kwargs)
        self.exc_info = None

    def run(self):
        try:
            super(ExcRecordingThread, self).run()
        except Exception:
            self.exc_info = sys.exc_info()


def make_exec_threaded(method_name):
    def _exec_threaded(instance, abort):
        outer_handlers = get_thread_handlers()
        task_id = instance.id
        def thread_wrapped_task():
            with inner_thread_nested_setup(outer_handlers):
                try:
                    # Reload from db
                    from kettle.tasks import Task
                    task = Task._from_id(task_id)
                    getattr(task, method_name)()
                except Exception:
                    # TODO: Fix logging
                    print traceback.format_exc()
                    logbook.exception()
                    abort.set()
        thread = ExcRecordingThread(target=thread_wrapped_task, name=instance.__class__.__name__)
        thread.start()
        return thread
    return _exec_threaded

def thread_wait(thread, abort):
    try:
        while True:
            if abort.is_set():
                pass # TODO: Set some kinda timeout
            if thread.is_alive():
                thread.join(1)
            else:
                break
    except Exception:
        # TODO: Fix logging
        print traceback.format_exc()
        logbook.exception()
        abort.set()
