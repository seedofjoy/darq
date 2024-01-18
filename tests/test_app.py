import asyncio
import datetime
import logging
import re
from unittest.mock import call
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from darq import Darq
from darq.app import DarqConnectionError
from darq.connections import ArqRedis
from darq.worker import Task
from . import redis_settings


def assert_is_ctx(ctx):
    assert isinstance(ctx, dict)
    assert isinstance(ctx.get('redis'), ArqRedis)
    assert isinstance(ctx.get('job_id'), str)
    assert isinstance(ctx.get('metadata'), dict)


def parse_log(records):
    return re.sub(
        r'\d+.\d\ds', 'X.XXs',
        '\n'.join(r.message.strip() for r in records),
    )


def assert_worker_job_finished(
        *, records, job_id, function_name, result, args=None, kwargs=None,
):
    call_args = []
    if args:
        call_args.append(', '.join(map(str, args)))
    if kwargs:
        call_args.append(', '.join([
            f'{name}={param}' for name, param in kwargs.items()
        ]))
    call_str = ', '.join(call_args)
    job_id_str = f'{job_id}:' if job_id else ''
    assert (
        f'X.XXs → {job_id_str}{function_name}({call_str})\n'
        f'X.XXs ← {job_id_str}{function_name} ● {result}'
    ) in parse_log(records)


async def foobar(a: int, enqueue_self=False) -> int:
    if enqueue_self:
        await foobar.apply_async([a + 1], {}, job_id='enqueue_self')
    return 42 + a


async def cron_func():
    return 'ok'


async def test_darq_connect_disconnect(darq):
    assert darq.redis_pool is None
    await darq.disconnect()  # should not fail

    await darq.connect()
    assert isinstance(darq.redis_pool, ArqRedis)

    await darq.disconnect()
    assert darq.redis_pool is None


async def test_darq_not_connected(darq):
    foobar_task = darq.task(foobar)
    with pytest.raises(DarqConnectionError):
        await foobar_task.delay()


async def test_job_works_like_a_function(darq):
    foobar_task = darq.task(foobar)
    assert await foobar_task(2) == 44
    assert await foobar_task(a=5) == 47


async def test_task_decorator(darq, caplog, worker_factory):
    caplog.set_level(logging.INFO)

    foobar_task = darq.task(foobar)

    await darq.connect()

    await foobar_task.apply_async([], {'a': 1}, job_id='testing')

    worker = worker_factory(darq)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    assert (
        'X.XXs → testing:tests.test_app.foobar(a=1)\n'
        'X.XXs ← testing:tests.test_app.foobar ● 43'
    ) in parse_log(caplog.records)


async def test_task_parametrized(darq):
    assert len(darq.registry) == 0

    timeout = 4
    keep_result = 0.5
    max_tries = 92
    queue = 'my_queue'
    with_ctx = True
    foobar_task = darq.task(
        keep_result=keep_result, timeout=timeout,
        max_tries=max_tries, queue=queue, with_ctx=with_ctx,
    )(foobar)

    task_name = 'tests.test_app.foobar'
    assert len(darq.registry) == 1
    task = darq.registry.get(task_name)
    assert isinstance(task, Task)
    assert task.name == task_name
    assert task.coroutine == foobar_task
    assert task.timeout_s == timeout
    assert task.keep_result_s == keep_result
    assert task.with_ctx == with_ctx


async def test_task_self_enqueue(darq, caplog, worker_factory):
    caplog.set_level(logging.INFO)

    foobar_task = darq.task(foobar)

    await darq.connect()
    await foobar_task.apply_async([1], {'enqueue_self': True}, job_id='testing')

    worker = worker_factory(darq)
    await worker.main()

    assert worker.jobs_complete == 2
    assert (
        'X.XXs → testing:tests.test_app.foobar(1, enqueue_self=True)\n'
        'X.XXs ← testing:tests.test_app.foobar ● 43\n'
        'X.XXs → enqueue_self:tests.test_app.foobar(2)\n'
        'X.XXs ← enqueue_self:tests.test_app.foobar ● 44'
    ) in parse_log(caplog.records)


@pytest.mark.parametrize('func_args,func_kwargs,result', [
    ((1,), {}, 43),
    ((), {'a': 2}, 44),
])
async def test_on_job_callbacks(
        func_args, func_kwargs, result, caplog, worker_factory, arq_redis,
):
    caplog.set_level(logging.INFO)

    expected_metadata = {'test_var': 'ok'}

    async def prepublish_side_effect(
            metadata, task, args, kwargs, job_options,
    ):
        metadata.update(expected_metadata)
        assert isinstance(task, Task)
        assert args == list(func_args)
        assert kwargs == func_kwargs
        assert job_options == {
            'job_id': 'testing',
            'queue_name': None,
            'defer_until': None,
            'defer_by': None,
            'expires': datetime.timedelta(days=1),
            'job_try': None,
        }

    on_job_prerun_future = asyncio.Future()
    on_job_prerun_future.set_result(None)
    on_job_prerun = Mock(return_value=on_job_prerun_future)

    on_job_postrun_future = asyncio.Future()
    on_job_postrun_future.set_result(None)
    on_job_postrun = Mock(return_value=on_job_postrun_future)

    on_job_prepublish = Mock(side_effect=prepublish_side_effect)

    darq = Darq(
        redis_settings=redis_settings,
        burst=True,
        on_job_prerun=on_job_prerun,
        on_job_postrun=on_job_postrun,
        on_job_prepublish=on_job_prepublish,
    )

    foobar_task = darq.task(foobar)

    on_job_prepublish.assert_not_called()

    await darq.connect()
    job_id = 'testing'
    function_name = 'tests.test_app.foobar'
    await foobar_task.apply_async(func_args, func_kwargs, job_id=job_id)

    on_job_prepublish.assert_called_once()

    worker = worker_factory(darq)
    await worker.main()

    assert_worker_job_finished(
        records=caplog.records,
        job_id=job_id,
        function_name=function_name,
        result=result,
        args=func_args,
        kwargs=func_kwargs,
    )

    on_job_prerun.assert_called_once()
    call_args = on_job_prerun.call_args[0]
    assert len(call_args) == 4  # ctx, task, args, kwargs
    ctx = call_args[0]
    assert_is_ctx(ctx)
    assert ctx['metadata'] == expected_metadata
    assert isinstance(call_args[1], Task)
    assert call_args[1].name == function_name
    assert call_args[1].coroutine == foobar_task
    assert call_args[2] == func_args
    assert call_args[3] == func_kwargs

    on_job_postrun.assert_called_once()
    call_args = on_job_postrun.call_args[0]
    assert len(call_args) == 5  # ctx, task, args, kwargs, result
    ctx = call_args[0]
    assert_is_ctx(ctx)
    assert ctx['metadata'] == expected_metadata
    assert isinstance(call_args[1], Task)
    assert call_args[1].name == function_name
    assert call_args[1].coroutine == foobar_task
    assert call_args[2] == func_args
    assert call_args[3] == func_kwargs
    assert call_args[4] == result


@pytest.mark.parametrize(
    'darq_kwargs, task_kwargs, func_args, func_kwargs, apply_async_kwargs,'
    'expected_kwargs', [
        (
            {}, {}, (), {}, {},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': datetime.timedelta(days=1),
            },
        ),
        (
            {'default_job_expires': 3600}, {}, (), {}, {},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': 3600,
            },
        ),
        (
            {'default_job_expires': 3600}, {}, (), {}, {'expires': 32},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': 32,
            },
        ),
        (
            {}, {'expires': 12}, (), {}, {},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': 12,
            },
        ),
        (
            {}, {'expires': 12}, (), {}, {'expires': 200},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': 200,
            },
        ),
    ],
)
@patch('darq.connections.ArqRedis.enqueue_job')
async def test_enqueue_job_params(
        enqueue_job_patched,
        darq_kwargs, task_kwargs, func_args, func_kwargs, apply_async_kwargs,
        expected_kwargs,
        arq_redis,
):
    enqueue_job_patched.reset_mock()

    darq = Darq(redis_settings=redis_settings, **darq_kwargs)
    foobar_task = darq.task(foobar, **task_kwargs)
    await darq.connect()
    await foobar_task.apply_async(func_args, func_kwargs, **apply_async_kwargs)

    enqueue_job_patched.assert_called_once_with(
        'tests.test_app.foobar', func_args, func_kwargs,
        **expected_kwargs,
    )
    await darq.disconnect()


@pytest.mark.parametrize('task_kwargs,apply_async_queue,expected', [
    ({}, None, 'arq:queue'),
    ({'queue': 'my_q'}, None, 'my_q'),
    ({}, 'my_queue', 'my_queue'),
    ({'queue': 'my_q'}, 'new_q', 'new_q'),
])
async def test_task_queue(
        task_kwargs, apply_async_queue, expected,
        arq_redis, darq,
):
    foobar_task = darq.task(foobar, **task_kwargs)
    await darq.connect()

    job = await foobar_task.apply_async([], {}, queue=apply_async_queue)
    assert job._queue_name == expected
    await darq.disconnect()


@pytest.mark.parametrize('packages', [
    (['some.package']),
    (['some.package', 'another.package']),
])
@patch('darq.app.importlib.import_module')
def test_autodiscover_tasks(import_module_mock, darq, packages):
    darq.autodiscover_tasks(packages)
    assert import_module_mock.call_count == len(packages)
    calls = [call(p) for p in packages]
    import_module_mock.assert_has_calls(calls)


@pytest.mark.parametrize(
    'darq_kwargs, task_kwargs, apply_async_kwargs,'
    'expected_kwargs', [
        (
            {}, {}, {},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': datetime.timedelta(days=1),
            },
        ),
        (
            {'default_job_expires': None}, {}, {},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': None,
            },
        ),
        (
            {}, {'expires': None}, {},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': None,
            },
        ),
        (
            {}, {'expires': 111}, {'expires': 222},
            {
                'defer_by': None, 'defer_until': None, 'job_id': None,
                'job_try': None, 'queue_name': None,
                'expires': 222,
            },
        ),
    ],
)
@patch('darq.connections.ArqRedis.enqueue_job')
async def test_expires_param(
        enqueue_job_patched,
        darq_kwargs, task_kwargs, apply_async_kwargs,
        expected_kwargs,
        arq_redis,
):
    enqueue_job_patched.reset_mock()

    darq = Darq(redis_settings=redis_settings, **darq_kwargs)
    foobar_task = darq.task(foobar, **task_kwargs)
    await darq.connect()
    await foobar_task.apply_async(tuple(), {}, **apply_async_kwargs)

    enqueue_job_patched.assert_called_once_with(
        'tests.test_app.foobar', tuple(), {},
        **expected_kwargs,
    )
    await darq.disconnect()


async def foobar_with_ctx(ctx, a: int) -> int:
    return 42 + a + ctx['b']


@pytest.mark.parametrize('func_args,func_kwargs,func_ctx,result', [
    ((1,), {}, {'b': 1}, 44),
    ((), {'a': 2}, {'b': 1}, 45),
])
async def test_run_task_with_ctx(
        func_args, func_kwargs, func_ctx, result,
        arq_redis, caplog, worker_factory,
):
    caplog.set_level(logging.INFO)

    async def on_worker_startup(ctx):
        ctx.update(func_ctx)

    darq = Darq(
        redis_settings=redis_settings,
        burst=True,
        on_startup=on_worker_startup,
    )

    foobar_with_ctx_task = darq.task(foobar_with_ctx, with_ctx=True)

    await darq.connect()

    job_id = 'testing'
    function_name = 'tests.test_app.foobar_with_ctx'
    await foobar_with_ctx_task.apply_async(
        func_args, func_kwargs, job_id=job_id,
    )

    worker = worker_factory(darq)
    await worker.main()

    assert_worker_job_finished(
        records=caplog.records,
        job_id=job_id,
        function_name=function_name,
        result=result,
        args=func_args,
        kwargs=func_kwargs,
    )
