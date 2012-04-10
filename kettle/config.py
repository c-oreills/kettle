import random

from tasks import ParallelExecTask, SequentialExecTask

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

def gradual_run(task_cls, delay_gen, arg_kwargs_list):
    return gradual_run_generic(
            task_cls, delay_gen, arg_kwargs_list, SequentialExecTask)

def gradual_run_parallel(task_cls, delay_gen, arg_kwargs_list):
    return gradual_run_generic(
            task_cls, delay_gen, arg_kwargs_list, ParallelExecTask)

def gradual_run_generic(task_cls, delay_gen, arg_kwargs_list, run_task_cls):
    picker = make_random_picker(arg_kwargs_list)
    def make_steps_fn(num):
        tasks = [task_cls(*args, **kwargs)
                for (args, kwargs) in picker(num)]
        if len(tasks) <= 1:
            return tasks
        else:
            return [(run_task_cls, (tasks,))]
    steps = []
    for num in 'one', 'half', 'all':
        next_steps = make_steps_fn(num)
        if next_steps:
            steps.extend(next_steps)
            if num != 'all':
                steps.append(delay_gen.next())
    return SequentialExecTask(filter(None, steps)) # Remove no-op steps
