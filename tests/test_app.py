import logging
import re
from unittest.mock import MagicMock

import arq
import pytest
from arq.connections import ArqRedis

from darq import Darq
from darq.app import DarqConnectionError
from . import redis_settings

darq_config = {'redis_settings': redis_settings, 'burst': True}


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


def assert_is_ctx(ctx):
    assert isinstance(ctx, dict)
    assert isinstance(ctx.get('redis'), ArqRedis)
    assert isinstance(ctx.get('job_id'), str)


def parse_log(records):
    return re.sub(
        r'\d+.\d\ds', 'X.XXs',
        '\n'.join(r.message.strip() for r in records),
    )


def assert_worker_job_finished(
        *, records, job_id, function_name, result, args, kwargs,
):
    call_args = []
    if args:
        call_args.append(', '.join(map(str, args)))
    if kwargs:
        call_args.append(', '.join([
            f'{name}={param}' for name, param in kwargs.items()
        ]))
    call_str = ', '.join(call_args)
    assert (
        f'X.XXs → {job_id}:{function_name}({call_str})\n'
        f'X.XXs ← {job_id}:{function_name} ● {result}'
    ) in parse_log(records)


async def foobar(a: int, enqueue_self=False) -> int:
    if enqueue_self:
        await foobar.delay(a + 1, _job_id='enqueue_self')
    return 42 + a


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
@pytest.mark.parametrize('args,kwargs,result', [
    ((1,), {}, 43),
    ((), {'a': 2}, 44),
])
async def test_on_job_callbacks(args, kwargs, result, caplog, worker_factory):
    caplog.set_level(logging.INFO)

    on_job_prerun = AsyncMock()
    on_job_prerun.__bool__ = lambda self: True

    on_job_postrun = AsyncMock()
    on_job_postrun.__bool__ = lambda self: True

    darq = Darq(
        darq_config,
        on_job_prerun=on_job_prerun,
        on_job_postrun=on_job_postrun,
    )

    foobar_task = darq.task(foobar)

    await darq.connect()
    job_id = 'testing'
    await foobar_task.delay(*args, _job_id=job_id, **kwargs)

    worker = await worker_factory(darq)
    await worker.main()

    assert_worker_job_finished(
        records=caplog.records,
        job_id=job_id,
        function_name='tests.test_app.foobar',
        result=result,
        args=args,
        kwargs=kwargs,
    )

    on_job_prerun.assert_called_once()
    call_args = on_job_prerun.call_args[0]
    assert len(call_args) == 4  # ctx, function, args, kwargs
    assert_is_ctx(call_args[0])
    assert call_args[1] == foobar_task
    assert call_args[2] == args
    assert call_args[3] == kwargs

    on_job_postrun.assert_called_once()
    call_args = on_job_postrun.call_args[0]
    assert len(call_args) == 5  # ctx, function, args, kwargs, result
    assert_is_ctx(call_args[0])
    assert call_args[1] == foobar_task
    assert call_args[2] == args
    assert call_args[3] == kwargs
    assert call_args[4] == result
