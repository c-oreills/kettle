from os import path

from logbook import NestedSetup
from logbook.handlers import Handler

import settings

def get_thread_handlers():
    # Return a list of log handlers handling this thread
    # They are reversed so that NestedSetup(get_thread_handlers()) gives an
    # identical handler to the ones already in place
    handlers = list(Handler.stack_manager.iter_context_objects())
    handlers.reverse()
    return handlers

def inner_thread_nested_setup(outer_handlers):
    return NestedSetup([h for h in outer_handlers if h not in get_thread_handlers()])

def log_filename(*args):
    return path.join(settings.LOG_DIR, '.'.join(map(str, args)))
