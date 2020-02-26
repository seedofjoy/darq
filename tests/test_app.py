import logging
import re

import arq
import pytest
from arq.connections import ArqRedis

from darq import Darq
from darq.app import DarqConnectionError
from . import redis_settings

darq_config = {'redis_settings': redis_settings, 'burst': True}


async def foobar(a: int) -> int:
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
async def test_task_decorator(caplog, arq_redis, worker_factory):
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

    log = re.sub(
        r'\d+.\d\ds', 'X.XXs',
        '\n'.join(r.message.strip() for r in caplog.records),
    )
    assert (
        'X.XXs → testing:tests.test_app.foobar(a=1)\n'
        'X.XXs ← testing:tests.test_app.foobar ● 43'
    ) in log


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
    assert arq_func.coroutine == foobar_task
    assert arq_func.timeout_s == timeout
    assert arq_func.keep_result_s == keep_result
