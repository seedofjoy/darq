import datetime
import logging
import re

import arq
import pytest
from arq.connections import ArqRedis
from asynctest import CoroutineMock
from asynctest import patch

from darq import cron
from darq import Darq
from darq.app import DarqConnectionError
from darq.app import DarqException
from . import redis_settings

darq_config = {'redis_settings': redis_settings, 'burst': True}


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
        await foobar.delay(a + 1, _job_id='enqueue_self')
    return 42 + a


async def cron_func():
    return 'ok'


@pytest.mark.asyncio
async def test_darq_connect_disconnect():
    darq = Darq(darq_config)
    assert not darq.connected
    assert darq.redis is None

    await darq.connect()
    assert darq.connected
    assert isinstance(darq.redis, ArqRedis)

    await darq.disconnect()
    assert not darq.connected
    assert darq.redis and darq.redis.connection.closed


@pytest.mark.asyncio
async def test_darq_not_connected():
    darq = Darq(darq_config)
    foobar_task = darq.task(foobar)
    with pytest.raises(DarqConnectionError):
        await foobar_task.delay()


@pytest.mark.asyncio
async def test_job_works_like_a_function():
    darq = Darq(darq_config)
    foobar_task = darq.task(foobar)
    assert await foobar_task(2) == 44
    assert await foobar_task(a=5) == 47


@pytest.mark.asyncio
async def test_task_decorator(caplog, worker_factory):
    caplog.set_level(logging.INFO)

    darq = Darq(darq_config)

    foobar_task = darq.task(foobar)

    await darq.connect()

    await foobar_task.delay(a=1, _job_id='testing')

    worker = await worker_factory(darq)
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


@pytest.mark.asyncio
async def test_task_parametrized():
    darq = Darq(darq_config)

    assert len(darq.registry) == 0

    timeout = 4
    keep_result = 0.5
    max_tries = 92
    queue = 'my_queue'
    foobar_task = darq.task(
        keep_result=keep_result, timeout=timeout,
        max_tries=max_tries, queue=queue,
    )(foobar)

    func_name = 'tests.test_app.foobar'
    assert len(darq.registry) == 1
    arq_func = darq.registry.get(func_name)
    assert isinstance(arq_func, arq.worker.Function)
    assert arq_func.name == func_name
    assert arq_func.coroutine.__wrapped__ == foobar_task
    assert arq_func.timeout_s == timeout
    assert arq_func.keep_result_s == keep_result


@pytest.mark.asyncio
async def test_task_self_enqueue(caplog, worker_factory):
    caplog.set_level(logging.INFO)

    darq = Darq(darq_config)

    foobar_task = darq.task(foobar)

    await darq.connect()
    await foobar_task.delay(1, _job_id='testing', enqueue_self=True)

    worker = await worker_factory(darq)
    await worker.main()

    assert worker.jobs_complete == 2
    assert (
        'X.XXs → testing:tests.test_app.foobar(1, enqueue_self=True)\n'
        'X.XXs ← testing:tests.test_app.foobar ● 43\n'
        'X.XXs → enqueue_self:tests.test_app.foobar(2)\n'
        'X.XXs ← enqueue_self:tests.test_app.foobar ● 44'
    ) in parse_log(caplog.records)


@pytest.mark.asyncio
@pytest.mark.parametrize('func_args,func_kwargs,result', [
    ((1,), {}, 43),
    ((), {'a': 2}, 44),
])
async def test_on_job_callbacks(
        func_args, func_kwargs, result, caplog, worker_factory,
):
    caplog.set_level(logging.INFO)

    expected_metadata = {'test_var': 'ok'}

    async def prepublish_side_effect(metadata, arq_function, args, kwargs):
        metadata.update(expected_metadata)
        assert isinstance(arq_function, arq.worker.Function)
        assert args == func_args
        assert kwargs == {
            '_expires': datetime.timedelta(days=1),
            '_job_id': 'testing',
            **func_kwargs,
        }

    on_job_prerun = CoroutineMock()
    on_job_postrun = CoroutineMock()
    on_job_prepublish = CoroutineMock(side_effect=prepublish_side_effect)

    darq = Darq(
        darq_config,
        on_job_prerun=on_job_prerun,
        on_job_postrun=on_job_postrun,
        on_job_prepublish=on_job_prepublish,
    )

    foobar_task = darq.task(foobar)

    on_job_prepublish.assert_not_called()

    await darq.connect()
    job_id = 'testing'
    function_name = 'tests.test_app.foobar'
    await foobar_task.delay(*func_args, _job_id=job_id, **func_kwargs)

    on_job_prepublish.assert_called_once()

    worker = await worker_factory(darq)
    await worker.main()

    assert_worker_job_finished(
        records=caplog.records,
        job_id=job_id,
        function_name=function_name,
        result=result,
        args=func_args,
        kwargs={'__metadata__': expected_metadata, **func_kwargs},
    )

    on_job_prerun.assert_called_once()
    call_args = on_job_prerun.call_args[0]
    assert len(call_args) == 4  # ctx, arq_function, args, kwargs
    ctx = call_args[0]
    assert_is_ctx(ctx)
    assert ctx['metadata'] == expected_metadata
    assert isinstance(call_args[1], arq.worker.Function)
    assert call_args[1].name == function_name
    assert call_args[1].coroutine.__wrapped__ == foobar_task
    assert call_args[2] == func_args
    assert call_args[3] == func_kwargs

    on_job_postrun.assert_called_once()
    call_args = on_job_postrun.call_args[0]
    assert len(call_args) == 5  # ctx, arq_function, args, kwargs, result
    ctx = call_args[0]
    assert_is_ctx(ctx)
    assert ctx['metadata'] == expected_metadata
    assert isinstance(call_args[1], arq.worker.Function)
    assert call_args[1].name == function_name
    assert call_args[1].coroutine.__wrapped__ == foobar_task
    assert call_args[2] == func_args
    assert call_args[3] == func_kwargs
    assert call_args[4] == result


@pytest.mark.asyncio
async def test_add_cron_jobs(caplog, worker_factory):
    caplog.set_level(logging.INFO)
    darq = Darq(darq_config)

    with pytest.raises(DarqException):
        darq.add_cron_jobs(cron(cron_func))

    cron_func_task = darq.task(cron_func)
    darq.add_cron_jobs(
        cron('tests.test_app.cron_func', run_at_startup=True),
        cron(cron_func, run_at_startup=True),
        cron(cron_func, run_at_startup=True),  # duplicate doesn't start
        cron(cron_func, name='custom_name', run_at_startup=True),
        cron(cron_func_task, name='custom_name2', run_at_startup=True),
    )

    worker = await worker_factory(darq)
    assert worker.jobs_complete == 0
    await worker.main()
    assert worker.jobs_complete == 4

    assert_worker_job_finished(
        records=caplog.records,
        job_id='cron',
        function_name='tests.test_app.cron_func',
        result="'ok'",
    )
    assert_worker_job_finished(
        records=caplog.records,
        job_id='cron',
        function_name='cron_func',
        result="'ok'",
    )
    assert_worker_job_finished(
        records=caplog.records,
        job_id='',
        function_name='custom_name',
        result="'ok'",
    )
    assert_worker_job_finished(
        records=caplog.records,
        job_id='',
        function_name='custom_name2',
        result="'ok'",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'darq_kwargs, task_kwargs, delay_args, delay_kwargs, expected_kwargs', [
        (
            {}, {}, [], {},
            {'_expires': datetime.timedelta(days=1)},
        ),
        (
            {'default_job_expires': 3600}, {}, [], {},
            {'_expires': 3600},
        ),
        (
            {'default_job_expires': 3600}, {}, [], {'_expires': 32},
            {'_expires': 32},
        ),
        (
            {}, {'expires': 12}, [], {},
            {'_expires': 12},
        ),
        (
            {}, {'expires': 12}, [], {'_expires': 200},
            {'_expires': 200},
        ),
    ],
)
@patch('arq.connections.ArqRedis.enqueue_job')
async def test_enqueue_job_params(
        enqueue_job_patched,
        darq_kwargs, task_kwargs, delay_args, delay_kwargs, expected_kwargs,
):
    enqueue_job_patched.reset_mock()

    darq = Darq(darq_config, **darq_kwargs)
    foobar_task = darq.task(foobar, **task_kwargs)
    await darq.connect()
    await foobar_task.delay(*delay_args, **delay_kwargs)

    enqueue_job_patched.assert_called_once_with(
        'tests.test_app.foobar',
        **expected_kwargs,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize('task_kwargs,delay_kwargs,expected', [
    ({}, {}, 'arq:queue'),
    ({'queue': 'my_q'}, {}, 'my_q'),
    ({}, {'_queue_name': 'my_queue'}, 'my_queue'),
    ({'queue': 'my_q'}, {'_queue_name': 'new_q'}, 'new_q'),
])
async def test_task_queue(task_kwargs, delay_kwargs, expected):
    darq = Darq(darq_config)
    foobar_task = darq.task(foobar, **task_kwargs)
    await darq.connect()

    job = await foobar_task.delay(**delay_kwargs)
    assert job._queue_name == expected
