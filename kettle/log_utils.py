from logbook import NestedSetup
from logbook.handlers import Handler

def get_thread_handlers():
    return list(Handler.stack_manager.iter_context_objects())

def inner_thread_nested_setup(outer_handlers):
    return NestedSetup(reversed(
        [h for h in outer_handlers if h not in get_thread_handlers()]))
