from darq.registry import Registry
from darq.worker import Task


async def foobar():
    pass


async def foobar_2():
    pass


def test_registry():
    registry = Registry()
    task_name = 'foobar_task'
    worker_task = Task(
        name=task_name, coroutine=foobar, timeout_s=None, keep_result_s=None,
        max_tries=None, with_ctx=False,
    )
    task_name_2 = 'foobar_2_task'
    worker_task_2 = Task(
        name=task_name_2, coroutine=foobar_2, timeout_s=None,
        keep_result_s=None, max_tries=None, with_ctx=False,
    )

    registry.add(worker_task)
    registry.add(worker_task_2)
    assert registry.get(task_name) == worker_task
    assert registry.get(task_name_2) == worker_task_2
    assert set(registry.get_functions()) == {worker_task, worker_task_2}

    task_names = registry.get_function_names()
    assert set(task_names) == {task_name, task_name_2}
