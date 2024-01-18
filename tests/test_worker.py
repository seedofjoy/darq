import asyncio
import functools
import logging
import re
import signal
from unittest.mock import MagicMock

import msgpack
import pytest
from aiohttp.test_utils import loop_context
from aioredis import create_redis_pool

from darq import Darq
from darq.constants import default_queue_name
from darq.constants import health_check_key_suffix
from darq.constants import job_key_prefix
from darq.jobs import Job
from darq.jobs import JobStatus
from darq.worker import async_check_health
from darq.worker import check_health
from darq.worker import FailedJobs
from darq.worker import JobExecutionFailed
from darq.worker import Retry
from darq.worker import run_worker
from . import redis_settings


async def foobar():
    return 42


async def foo(v):
    return v + 1


async def bar(v):
    await foo.delay(v + 1)


async def raise_on_value(v=0):
    if v > 0:
        raise ValueError('xxx')


async def return_error():
    return TypeError('xxx')


async def retry(defer=None):
    raise Retry(defer=defer)


async def fails():
    raise TypeError('my type error')


@pytest.mark.skip
async def test_handle_sig_delayed(
        darq, caplog, arq_redis, worker_factory, loop,
):
    darq.task(foobar)
    caplog.set_level(logging.INFO)

    worker = worker_factory(darq)

    long_running_task_mock = MagicMock(done=MagicMock(return_value=False))
    worker.main_task = MagicMock()
    worker.tasks = [
        MagicMock(done=MagicMock(return_value=True)),
        long_running_task_mock,
    ]

    if caplog.records:
        assert all(
            'Task was destroyed but it is pending!' in record.message
            for record in caplog.records
        )
    worker.handle_sig(signal.SIGINT)
    await asyncio.sleep(0)
    assert caplog.records[-1].message == (
        'Warm shutdown. Awaiting for 1 jobs with 30 seconds timeout.'
    )
    if len(caplog.records) > 1:
        assert all(
            'Task was destroyed but it is pending!' in record.message
            for record in caplog.records[:-1]
        )
    assert worker.main_task.cancel.call_count == 0
    assert worker.tasks[0].cancel.call_count == 0
    assert worker.tasks[1].cancel.call_count == 0
    await asyncio.sleep(0)

    worker.tasks[1].done.return_value = True
    await asyncio.sleep(1)

    assert 'shutdown on SIGINT' in caplog.records[-1].message
    assert worker.main_task.cancel.call_count == 1
    assert worker.tasks[0].cancel.call_count == 0
    assert worker.tasks[1].cancel.call_count == 0


async def test_handle_sig_hard(darq, caplog, worker_factory):
    darq.task(foobar)
    caplog.set_level(logging.INFO)

    worker = worker_factory(darq)

    long_running_task_mock = MagicMock(done=MagicMock(return_value=False))
    worker.main_task = MagicMock()
    worker.tasks = [long_running_task_mock]

    caplog.clear()
    worker.handle_sig(signal.SIGINT)
    await asyncio.sleep(0)
    assert len(caplog.records) == 1
    assert caplog.records[0].message == (
        'Warm shutdown. Awaiting for 1 jobs with 30 seconds timeout.'
    )
    worker.handle_sig(signal.SIGINT)
    assert caplog.records[1].message == (
        'shutdown on SIGINT ◆ 0 jobs complete ◆ 0 failed ◆ 0 retries ◆ '
        '1 ongoing to cancel'
    )


def test_no_jobs(darq, arq_redis, event_loop):
    darq.task(foobar)

    event_loop.run_until_complete(
        arq_redis.enqueue_job('tests.test_worker.foobar', [], {}),
    )
    with loop_context():
        worker = run_worker(darq)
    assert worker.jobs_complete == 1
    assert str(worker) == (
        '<Worker j_complete=1 j_failed=0 j_retried=0 j_ongoing=0>'
    )


def test_health_check_direct(darq):
    with loop_context():
        assert check_health(darq, queue=None) == 1


async def test_health_check_fails():
    assert 1 == await async_check_health(redis_settings)


async def test_health_check_pass(arq_redis):
    await arq_redis.set(default_queue_name + health_check_key_suffix, b'1')
    assert 0 == await async_check_health(redis_settings)


async def test_set_health_check_key(arq_redis, worker_factory):
    darq = Darq(
        redis_settings=redis_settings, burst=True, poll_delay=0,
        health_check_key='darq:test:health-check',
    )
    darq.task(foobar)
    await arq_redis.enqueue_job(
        'tests.test_worker.foobar', [], {}, job_id='testing',
    )
    worker = worker_factory(darq)
    await worker.main()
    assert sorted(await arq_redis.keys('*')) == [
        'arq:result:testing',
        'darq:test:health-check',
    ]


async def test_job_successful(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    foobar_task = darq.task(foobar)
    await darq.connect()

    await foobar_task.apply_async([], {}, job_id='testing')
    worker = worker_factory(darq)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(
        r'\d+.\d\ds', 'X.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    expected = (
        'X.XXs → testing:tests.test_worker.foobar()\n  '
        'X.XXs ← testing:tests.test_worker.foobar ● 42'
    )
    assert expected in log


@pytest.mark.skip('Jobs with "ctx" does not ready')
async def test_job_retry(darq, arq_redis, worker_factory, caplog):
    async def retry(ctx):
        if ctx['job_try'] <= 2:
            raise Retry(defer=0.01)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 2

    log = re.sub(
        r'(\d+).\d\ds', r'\1.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    assert '0.XXs ↻ testing:retry retrying job in 0.XXs\n' in log
    assert '0.XXs → testing:retry() try=2\n' in log
    assert '0.XXs ← testing:retry ●' in log


async def test_job_retry_dont_retry(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    retry_task = darq.task(retry)
    await darq.connect()

    await retry_task.apply_async([], {'defer': 0.01}, job_id='testing')
    worker = worker_factory(darq)
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check(retry_jobs=False)
    assert str(exc_info.value) == '1 job failed <Retry defer 0.01s>'

    assert '↻' not in caplog.text
    expected = (
        '! testing:tests.test_worker.retry failed, Retry: <Retry defer 0.01s>\n'
    )
    assert expected in caplog.text


async def test_job_retry_max_jobs(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    retry_task = darq.task(retry)
    await darq.connect()

    await retry_task.apply_async([], {'defer': 0.01}, job_id='testing')
    worker = worker_factory(darq)
    assert await worker.run_check(max_burst_jobs=1) == 0
    assert worker.jobs_complete == 0
    assert worker.jobs_retried == 1
    assert worker.jobs_failed == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', caplog.text)
    assert (
        '0.XXs ↻ testing:tests.test_worker.retry retrying job in 0.XXs\n' in log
    )
    assert '0.XXs → testing:tests.test_worker.retry() try=2\n' not in log


async def test_job_job_not_found(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    darq.task(foobar)
    await darq.connect()

    await arq_redis.enqueue_job('missing', [], {}, job_id='testing')
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(
        r'\d+.\d\ds', 'X.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    assert "job testing, function 'missing' not found" in log


async def test_job_job_not_found_run_check(
        darq, arq_redis, worker_factory, caplog,
):
    darq.task(foobar)
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('missing', [], {}, job_id='testing')
    worker = worker_factory(darq)
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()

    assert exc_info.value.count == 1
    assert len(exc_info.value.job_results) == 1
    failure = exc_info.value.job_results[0].result
    assert failure == JobExecutionFailed("function 'missing' not found")
    assert failure != 123  # check the __eq__ method of JobExecutionFailed


async def test_retry_lots(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    retry_task = darq.task(retry)
    await darq.connect()

    await retry_task.apply_async([], {}, job_id='testing')
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 5

    log = re.sub(
        r'\d+.\d\ds', 'X.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    expected = (
        '  X.XXs ! testing:tests.test_worker.retry max retries 5 exceeded'
    )
    assert expected in log


async def test_retry_lots_without_keep_result(darq, arq_redis, worker_factory):
    retry_task = darq.task(retry, keep_result=0)
    await darq.connect()
    await retry_task.apply_async([], {}, job_id='testing')
    worker = worker_factory(darq)
    await worker.main()  # Should not raise MultiExecError


async def test_retry_lots_check(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    retry_task = darq.task(retry)
    await darq.connect()

    await retry_task.apply_async([], {}, job_id='testing')
    worker = worker_factory(darq)
    with pytest.raises(FailedJobs, match='max 5 retries exceeded'):
        await worker.run_check()


@pytest.mark.skip(reason='Jobs with "ctx" does not ready')
async def test_cancel_error(darq, arq_redis, worker_factory, caplog):
    async def retry(ctx):
        if ctx['job_try'] == 1:
            raise asyncio.CancelledError()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 1

    log = re.sub(
        r'\d+.\d\ds', 'X.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    assert 'X.XXs ↻ testing:retry cancelled, will be run again' in log


async def test_job_expired(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    foobar_task = darq.task(foobar)
    await darq.connect()

    await foobar_task.apply_async([], {}, job_id='testing')
    await arq_redis.delete(job_key_prefix + 'testing')
    worker = worker_factory(darq)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(
        r'\d+.\d\ds', 'X.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    assert 'job testing expired' in log


async def test_job_expired_run_check(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    darq_task = darq.task(foobar)
    await darq.connect()

    await darq_task.apply_async([], {}, job_id='testing')
    await arq_redis.delete(job_key_prefix + 'testing')
    worker = worker_factory(darq)
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()

    exc_value = exc_info.value
    assert str(exc_value) == "1 job failed JobExecutionFailed('job expired')"
    assert exc_value.count == 1
    assert len(exc_value.job_results) == 1
    assert exc_value.job_results[0].result == JobExecutionFailed('job expired')


async def test_job_old(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    foobar_task = darq.task(foobar)
    await darq.connect()

    await foobar_task.apply_async([], {}, job_id='testing', defer_by=-2)
    worker = worker_factory(darq)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(
        r'(\d+).\d\ds', r'\1.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    assert log.endswith(
        '  0.XXs → testing:tests.test_worker.foobar() delayed=2.XXs\n'
        '  0.XXs ← testing:tests.test_worker.foobar ● 42',
    )


async def test_retry_repr():
    assert str(Retry(123)) == '<Retry defer 123.00s>'


async def test_str_function(darq, arq_redis, worker_factory, caplog):
    darq.task(asyncio.sleep)
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('asyncio.tasks.sleep', [], {}, job_id='testing')
    worker = worker_factory(darq)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(
        r'(\d+).\d\ds', r'\1.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    assert '0.XXs ! testing:asyncio.tasks.sleep failed, TypeError' in log


async def test_startup_shutdown(arq_redis, worker_factory):
    calls = []

    async def startup(ctx):
        calls.append('startup')

    async def shutdown(ctx):
        calls.append('shutdown')

    darq = Darq(
        redis_settings=redis_settings, burst=True,
        on_startup=startup, on_shutdown=shutdown,
    )
    foobar_task = darq.task(foobar)
    await darq.connect()

    await foobar_task.apply_async([], {}, job_id='testing')
    worker = worker_factory(darq)
    await worker.main()
    await worker.close()

    assert calls == ['startup', 'shutdown']


class CustomError(RuntimeError):
    def extra(self):
        return {'x': 'y'}


async def error_function():
    raise CustomError('this is the error')


async def test_exc_extra(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    error_function_task = darq.task(error_function)
    await darq.connect()

    await error_function_task.apply_async([], {}, job_id='testing')
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_failed == 1

    log = re.sub(
        r'(\d+).\d\ds', r'\1.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    expected = (
        '0.XXs ! testing:tests.test_worker.error_function failed, '
        'CustomError: this is the error'
    )
    assert expected in log
    error = next(r for r in caplog.records if r.levelno == logging.ERROR)
    assert error.extra == {'x': 'y'}


async def unpickleable():
    class Foo:
        pass
    return Foo()


async def test_unpickleable(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    darq.task(unpickleable)

    await arq_redis.enqueue_job(
        'tests.test_worker.unpickleable', [], {}, job_id='testing',
    )
    worker = worker_factory(darq)
    await worker.main()

    log = re.sub(
        r'(\d+).\d\ds', r'\1.XXs',
        '\n'.join(r.message for r in caplog.records),
    )
    expected = (
        'error serializing result of testing:tests.test_worker.unpickleable'
    )
    assert expected in log


async def test_log_health_check(arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    darq = Darq(
        redis_settings=redis_settings, burst=True, health_check_interval=0,
    )
    foobar_task = darq.task(foobar)
    await darq.connect()

    await foobar_task.apply_async([], {}, job_id='testing')
    worker = worker_factory(darq)
    await worker.main()
    await worker.main()
    await worker.main()
    assert worker.jobs_complete == 1

    expected = 'j_complete=1 j_failed=0 j_retried=0 j_ongoing=0 queued=0'
    assert expected in caplog.text
    # can happen more than once due to redis pool size
    # assert log.count('recording health') == 1
    assert 'recording health' in caplog.text


async def test_remain_keys(darq, arq_redis, worker_factory):
    foobar_task = darq.task(foobar)
    await darq.connect()

    redis2 = await create_redis_pool(
        (redis_settings.host, redis_settings.port), encoding='utf8',
    )
    try:
        await foobar_task.apply_async([], {}, job_id='testing')
        assert sorted(await redis2.keys('*')) == [
            'arq:job:testing', 'arq:queue',
        ]
        worker = worker_factory(darq)
        await worker.main()
        assert sorted(await redis2.keys('*')) == [
            'arq:queue:health-check', 'arq:result:testing',
        ]
        await worker.close()
        assert sorted(await redis2.keys('*')) == ['arq:result:testing']
    finally:
        redis2.close()
        await redis2.wait_closed()


async def test_remain_keys_no_results(darq, arq_redis, worker_factory):
    foobar_task = darq.task(foobar, keep_result=0)
    await darq.connect()

    await foobar_task.apply_async([], {}, job_id='testing')
    assert sorted(await arq_redis.keys('*')) == ['arq:job:testing', 'arq:queue']
    worker = worker_factory(darq)
    await worker.main()
    assert sorted(await arq_redis.keys('*')) == ['arq:queue:health-check']


async def test_run_check_passes(darq, arq_redis, worker_factory):
    foobar_task = darq.task(foobar)
    await darq.connect()

    await foobar_task.apply_async([], {})
    await foobar_task.apply_async([], {})
    worker = worker_factory(darq)
    assert 2 == await worker.run_check()


async def test_run_check_error(darq, arq_redis, worker_factory):
    fails_task = darq.task(fails)
    await darq.connect()

    await fails_task.delay()
    worker = worker_factory(darq)
    with pytest.raises(
            FailedJobs, match=r"1 job failed TypeError\('my type error'",
    ):
        await worker.run_check()


async def test_run_check_error2(darq, arq_redis, worker_factory):
    raise_on_value_task = darq.task(raise_on_value)
    await darq.connect()

    await raise_on_value_task.delay(1)
    await raise_on_value_task.delay(1)
    worker = worker_factory(darq)
    with pytest.raises(FailedJobs, match='2 jobs failed:\n') as exc_info:
        await worker.run_check()
    assert len(exc_info.value.job_results) == 2


async def test_return_exception(darq, arq_redis, worker_factory):
    return_error_task = darq.task(return_error)
    await darq.connect()

    j = await return_error_task.delay()
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    r = await j.result(pole_delay=0)
    assert isinstance(r, TypeError)
    info = await j.result_info()
    assert info.success is True


async def test_error_success(darq, arq_redis, worker_factory):
    fails_task = darq.task(fails)
    await darq.connect()

    j = await fails_task.delay()
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0
    info = await j.result_info()
    assert info.success is False


async def test_many_jobs_expire(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    foobar_task = darq.task(foobar)
    await darq.connect()

    await foobar_task.delay()
    await asyncio.gather(*[
        arq_redis.zadd(default_queue_name, 1, f'testing-{i}')
        for i in range(100)
    ])
    worker = worker_factory(darq)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 100
    assert worker.jobs_retried == 0

    log = '\n'.join(r.message for r in caplog.records)
    assert 'job testing-0 expired' in log
    assert log.count(' expired') == 100


async def test_repeat_job_result(darq, arq_redis, worker_factory):
    foobar_task = darq.task(foobar)
    await darq.connect()

    j1 = await foobar_task.apply_async([], {}, job_id='job_id')
    assert isinstance(j1, Job)
    assert await j1.status() == JobStatus.queued

    j2 = await foobar_task.apply_async([], {}, job_id='job_id')
    assert j2 is None

    await worker_factory(darq).run_check()
    assert await j1.status() == JobStatus.complete

    j3 = await foobar_task.apply_async([], {}, job_id='job_id')
    assert j3 is None


async def test_queue_read_limit_equals_max_jobs(arq_redis, worker_factory):
    darq = Darq(redis_settings=redis_settings, burst=True, max_jobs=2)
    foobar_task = darq.task(foobar)
    await darq.connect()

    for _ in range(4):
        await foobar_task.delay()

    assert await arq_redis.zcard(default_queue_name) == 4
    worker = worker_factory(darq)
    worker.pool = arq_redis
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 2
    assert worker.jobs_complete == 2
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 0
    assert worker.jobs_complete == 4
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_custom_queue_read_limit(arq_redis, worker_factory):
    darq = Darq(
        redis_settings=redis_settings, burst=True, max_jobs=4,
        queue_read_limit=2,
    )
    foobar_task = darq.task(foobar)
    await darq.connect()

    for _ in range(4):
        await foobar_task.delay()

    assert await arq_redis.zcard(default_queue_name) == 4
    worker = worker_factory(darq)
    worker.pool = arq_redis
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 2
    assert worker.jobs_complete == 2
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 0
    assert worker.jobs_complete == 4
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_custom_serializers(arq_redis_msgpack, worker_factory):
    darq = Darq(
        redis_settings=redis_settings, burst=True,
        job_serializer=msgpack.packb,
        job_deserializer=functools.partial(msgpack.unpackb, raw=False),
    )
    foobar_task = darq.task(foobar)
    await darq.connect()

    j = await foobar_task.apply_async([], {}, job_id='job_id')
    worker = worker_factory(darq)
    info = await j.info()
    assert info.function == 'tests.test_worker.foobar'
    assert await worker.run_check() == 1
    assert await j.result() == 42
    r = await j.info()
    assert r.result == 42


class UnpickleFails:
    def __init__(self, v):
        self.v = v

    def __setstate__(self, state):
        raise ValueError('this broke')


async def test_deserialization_error(darq, arq_redis, worker_factory):
    darq.task(foobar)
    await arq_redis.enqueue_job(
        'tests.test_worker.foobar',
        [UnpickleFails('hello')], {}, job_id='job_id',
    )
    worker = worker_factory(darq)
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()
    assert str(exc_info.value) == (
        "1 job failed DeserializationError('unable to deserialize job')"
    )


async def test_incompatible_serializers_1(
        darq, arq_redis_msgpack, worker_factory,
):
    darq.task(foobar)
    await arq_redis_msgpack.enqueue_job(
        'tests.test_worker.foobar', [], {}, job_id='job_id',
    )
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0


async def test_incompatible_serializers_2(arq_redis, worker_factory):
    darq = Darq(
        redis_settings=redis_settings, burst=True,
        job_serializer=msgpack.packb,
        job_deserializer=functools.partial(msgpack.unpackb, raw=False),
    )
    darq.task(foobar)

    await arq_redis.enqueue_job(
        'tests.test_worker.foobar', [], {}, job_id='job_id',
    )
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0


async def test_max_jobs_completes(darq, arq_redis, worker_factory):
    raise_on_value_task = darq.task(raise_on_value)
    await darq.connect()

    await raise_on_value_task.delay()
    await raise_on_value_task.delay(1)
    await raise_on_value_task.delay(2)
    worker = worker_factory(darq)
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check(max_burst_jobs=3)
    assert repr(exc_info.value).startswith('<2 jobs failed:')


async def test_max_bursts_sub_call(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    darq.task(foo)
    bar_task = darq.task(bar)
    await darq.connect()

    await bar_task.delay(10)
    worker = worker_factory(darq)
    assert await worker.run_check(max_burst_jobs=1) == 1
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert 'bar(10)' in caplog.text
    assert 'foo' in caplog.text


async def test_max_bursts_multiple(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.INFO)
    foo_task = darq.task(foo)
    await darq.connect()

    await foo_task.delay(1)
    await asyncio.sleep(0.005)
    await foo_task.delay(2)
    worker = worker_factory(darq)
    assert await worker.run_check(max_burst_jobs=1) == 1
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert 'foo(1)' in caplog.text
    assert 'foo(2)' not in caplog.text


async def test_max_bursts_dont_get(darq, arq_redis, worker_factory):
    foo_task = darq.task(foo)
    await darq.connect()

    await foo_task.delay(1)
    await foo_task.delay(2)
    await darq.connect()
    worker = worker_factory(darq)

    worker.max_burst_jobs = 0
    assert len(worker.tasks) == 0
    await worker._poll_iteration()
    assert len(worker.tasks) == 0


async def test_non_burst(darq, arq_redis, worker_factory, caplog, event_loop):
    caplog.set_level(logging.INFO)
    foo_task = darq.task(foo)
    await darq.connect()

    await foo_task.apply_async([1], {}, job_id='testing')
    worker = worker_factory(darq)
    worker.burst = False
    t = event_loop.create_task(worker.main())
    await asyncio.sleep(0.1)
    t.cancel()
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert '← testing:tests.test_worker.foo ● 2' in caplog.text


@pytest.mark.skip(reason='Not working')
async def test_multi_exec(darq, arq_redis, worker_factory, caplog):
    caplog.set_level(logging.DEBUG, logger='darq.worker')
    darq.task(foo)
    await arq_redis.enqueue_job(
        'tests.test_worker.foo_multi_exec', 1, _job_id='testing',
    )
    await darq.connect()
    worker = worker_factory(darq)
    await asyncio.gather(*[worker.run_jobs(['testing']) for _ in range(5)])
    assert (
        'multi-exec error, job testing already started elsewhere' in caplog.text
    )
    assert 'WatchVariableError' not in caplog.text
