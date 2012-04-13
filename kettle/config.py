import random

from tasks import ParallelExecTask, SequentialExecTask

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
    unpicked_items = list(items)
    picked_items = list()
    def picker(num):
        picks = pick_randomly(num, unpicked_items, picked_items)
        for pick in picks:
            unpicked_items.remove(pick)
            picked_items.append(pick)
        return picks
    return picker

def gradual_exec(rollout_id, task_cls, delay_gen, args_kwargs_list):
    return gradual_exec_generic(
            rollout_id, task_cls, delay_gen, args_kwargs_list, SequentialExecTask)

def gradual_exec_parallel(rollout_id, task_cls, delay_gen, args_kwargs_list):
    return gradual_exec_generic(
            rollout_id, task_cls, delay_gen, args_kwargs_list, ParallelExecTask)

def gradual_exec_generic(rollout_id, task_cls, delay_gen, args_kwargs_list, run_task_cls):
    picker = make_random_picker(args_kwargs_list)
    def make_steps_fn(num):
        tasks = [task_cls(*args, **kwargs)
                for (args, kwargs) in picker(num)]
        if len(tasks) <= 1:
            return tasks
        else:
            return [run_task_cls(rollout_id, tasks)]
    steps = []
    for num in 'one', 'half', 'all':
        next_steps = make_steps_fn(num)
        if next_steps:
            steps.extend(next_steps)
            if num != 'all':
                steps.append(delay_gen.next())
    return SequentialExecTask(rollout_id, filter(None, steps)) # Remove no-op steps
