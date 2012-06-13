from tasks import ParallelExecTask, SequentialExecTask

def num_to_pick(num, unprocessed, processed=None):
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
    return num_to_pick

def make_picker(items):
    items_unpicked = list(items)
    items_picked = list()
    def picker(num):
        num_picks = num_to_pick(num, items_unpicked, items_picked)
        picks = items_unpicked[:num_picks]
        items_picked.extend(picks)
        del items_unpicked[:num_picks]
        return picks
    return picker

def gradual_exec(rollout_id, task_cls, delay_gen, args_kwargs_list):
    return gradual_exec_generic(
            rollout_id, task_cls, delay_gen, args_kwargs_list, SequentialExecTask)

def gradual_exec_parallel(rollout_id, task_cls, delay_gen, args_kwargs_list):
    return gradual_exec_generic(
            rollout_id, task_cls, delay_gen, args_kwargs_list, ParallelExecTask)

def gradual_exec_generic(rollout_id, task_cls, delay_gen, args_kwargs_list, run_task_cls):
    picker = make_picker(args_kwargs_list)
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
