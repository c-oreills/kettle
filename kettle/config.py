from itertools import chain
import random

from tasks import ParallelStepsTask

def make_stage(name, steps):
    return {'name': name, 'steps': steps}

def pick_randomly(num, unprocessed, processed=None):
    if processed is None:
        len_processed = 0
    else:
        len_processed = len(processed)
    len_total = len_processed + len(unprocessed)
    text_nums = {
            'one': 1,
            'half': len_total/2,
            'all': len_total,
            }
    if num in text_nums:
        num = text_nums[num]
    num_to_pick = max(0, num - len_processed)
    return random.sample(unprocessed, num_to_pick)

def make_random_picker(items):
    unpicked_items = set(items)
    picked_items = set()
    def picker(num):
        picks = pick_randomly(num, unpicked_items, picked_items)
        for pick in picks:
            unpicked_items.remove(pick)
            picked_items.add(pick)
        return picks
    return picker

def gradual_rollout(task, servers, delay_gen, *args, **kwargs):
    server_picker = make_random_picker(servers)
    def make_steps(num):
        return [(task, (server,) + args, kwargs)
                for server in server_picker(num)]
    return gradual_rollout_generic(delay_gen, make_steps, *args, **kwargs)

def gradual_rollout_parallel(task, servers, delay_gen, *args, **kwargs):
    server_picker = make_random_picker(servers)
    def make_parallel_step(num):
        steps = [(task, (server,) + args, kwargs)
                    for server in server_picker(num)]
        if len(steps) <= 1:
            return steps
        else:
            return [(ParallelStepsTask, (steps,))]
    return gradual_rollout_generic(
            delay_gen, make_parallel_step, *args, **kwargs)

def gradual_rollout_generic(delay_gen, make_steps_fn, *args, **kwargs):
    steps = chain(
            make_steps_fn('one'),
            [delay_gen.next()],
            make_steps_fn('half'),
            [delay_gen.next()],
            make_steps_fn('all'),
            )
    return [step for step in steps if step]
