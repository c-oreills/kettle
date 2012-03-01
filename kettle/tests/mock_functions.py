# Functions to be used when testing Rollouts (functions must be picklable so
# can't be defined inline)

calls = []

def fn1(*args, **kwargs):
    calls.append((fn1, args, kwargs))

def fn2(*args, **kwargs):
    calls.append((fn2, args, kwargs))

def fn3(*args, **kwargs):
    calls.append((fn3, args, kwargs))

def fn_error(*args, **kwargs):
    calls.append((fn_error, args, kwargs))
    raise Exception

def clear_calls():
    del calls[:]
