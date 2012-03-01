from threading import Thread
import traceback


from log_utils import get_thread_handlers, inner_thread_nested_setup

def parse_step(step):
    try:
        task, args, kwargs = step
    except:
        if len(step) == 2:
            if isinstance(step[1], tuple):
                task, args = step
                kwargs = {}
            elif isinstance(step[1], dict):
                task, kwargs = step
                args = ()
            else:
                raise ValueError('Invalid step specification: %s' % step)
        elif len(step) == 1:
            (task,) = step
            args, kwargs = (), {}
        else:
            raise ValueError('Invalid step specification: %s' % step)
    return task, args, kwargs

def run_step_thread(rollout_id, step, abort_event, call_run=True):
    task, args, kwargs = parse_step(step)
    outer_handlers = get_thread_handlers()
    def thread_wrapped_task():
        with inner_thread_nested_setup(outer_handlers):
            try:
                task_call = task(rollout_id, *args, **kwargs)
                if call_run and hasattr(task, 'run'):
                    task_call.run()
            except Exception:
                # TODO: Log
                print traceback.format_exc()
                abort_event.set()
    thread = Thread(target=thread_wrapped_task, name=task.__name__)
    thread.start()
    return thread

def thread_wait(thread, abort_event):
    try:
        while True:
            if abort_event.is_set():
                pass # Set some kinda timeout on step_thread
            if thread.is_alive():
                thread.join(1)
            else:
                break
    except Exception:
        print traceback.format_exc()
        abort_event.set()

def friendly_step(step, friendly_type):
    task, args, kwargs = parse_step(step)
    if hasattr(task, friendly_type):
        return getattr(task, friendly_type)(*args, **kwargs)
    else:
        from kettle.tasks import Task
        return getattr(Task, friendly_type)(*args, **kwargs)

def friendly_step_html(step):
    return friendly_step(step, 'friendly_html')

def friendly_step_str(step):
    return friendly_step(step, 'friendly_str')
