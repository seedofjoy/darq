import asyncio
import pickle

import pytest

from darq.constants import default_queue_name
from darq.constants import in_progress_key_prefix
from darq.constants import job_key_prefix
from darq.constants import result_key_prefix
from darq.jobs import DeserializationError
from darq.jobs import deserialize_job_raw
from darq.jobs import Job
from darq.jobs import JobResult
from darq.jobs import JobStatus
from darq.jobs import serialize_result
from .utils import CloseToNow


async def test_job_in_progress(arq_redis):
    await arq_redis.set(in_progress_key_prefix + 'foobar', b'1')
    j = Job('foobar', arq_redis)
    assert JobStatus.in_progress == await j.status()
    assert str(j) == '<darq job foobar>'


async def test_unknown(arq_redis):
    j = Job('foobar', arq_redis)
    assert JobStatus.not_found == await j.status()
    info = await j.info()
    assert info is None


async def test_result_timeout(arq_redis):
    j = Job('foobar', arq_redis)
    with pytest.raises(asyncio.TimeoutError):
        await j.result(0.1, pole_delay=0)


async def foobar(*args, **kwargs):
    return 42


async def test_enqueue_job(
        darq, arq_redis, worker_factory, queue_name=default_queue_name,
):
    darq.task(foobar)

    j = await arq_redis.enqueue_job(
        'tests.test_jobs.foobar', [1, 2], {'c': 3}, queue_name=queue_name,
    )
    assert isinstance(j, Job)
    assert JobStatus.queued == await j.status()
    worker = worker_factory(darq, queue_name=queue_name)
    await worker.main()
    r = await j.result(pole_delay=0)
    assert r == 42
    assert JobStatus.complete == await j.status()
    info = await j.info()
    assert info == JobResult(
        job_try=1,
        function='tests.test_jobs.foobar',
        args=[1, 2],
        kwargs={'c': 3},
        enqueue_time=CloseToNow(),
        success=True,
        result=42,
        start_time=CloseToNow(),
        finish_time=CloseToNow(),
        score=None,
    )
    results = await arq_redis.all_job_results()
    assert results == [
        JobResult(
            function='tests.test_jobs.foobar',
            args=[1, 2],
            kwargs={'c': 3},
            job_try=1,
            enqueue_time=CloseToNow(),
            success=True,
            result=42,
            start_time=CloseToNow(),
            finish_time=CloseToNow(),
            score=None,
            job_id=j.job_id,
        ),
    ]


async def test_enqueue_job_alt_queue(darq, arq_redis, worker_factory):
    await test_enqueue_job(
        darq, arq_redis, worker_factory, queue_name='custom_queue',
    )


async def test_cant_unpickle_at_all():
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    r1 = serialize_result(
        'foobar', (1,), {}, 1, 123, True, Foobar(), 123, 123, 'testing',
    )
    assert isinstance(r1, bytes)
    r2 = serialize_result(
        'foobar', (Foobar(),), {}, 1, 123, True, Foobar(), 123, 123, 'testing',
    )
    assert r2 is None


async def test_custom_serializer():
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    def custom_serializer(x):
        return b'0123456789'

    r1 = serialize_result(
        'foobar', (1,), {}, 1, 123, True, Foobar(), 123, 123, 'testing',
        serializer=custom_serializer,
    )
    assert r1 == b'0123456789'
    r2 = serialize_result(
        'foobar', (Foobar(),), {}, 1, 123, True, Foobar(), 123, 123, 'testing',
        serializer=custom_serializer,
    )
    assert r2 == b'0123456789'


async def foobar_sum(a, b):
    return a + b


async def test_deserialize_result(darq, arq_redis, worker_factory):
    darq.task(foobar_sum)

    j = await arq_redis.enqueue_job('tests.test_jobs.foobar_sum', [1, 2], {})
    assert JobStatus.queued == await j.status()
    worker = worker_factory(darq)
    await worker.run_check()
    assert await j.result(pole_delay=0) == 3
    assert await j.result(pole_delay=0) == 3
    info = await j.info()
    assert info.args == [1, 2]
    await arq_redis.set(result_key_prefix + j.job_id, b'invalid pickle data')
    with pytest.raises(
            DeserializationError, match='unable to deserialize job result',
    ):
        assert await j.result(pole_delay=0) == 3


async def test_deserialize_info(arq_redis):
    j = await arq_redis.enqueue_job('foobar', [1, 2], {})
    assert JobStatus.queued == await j.status()
    await arq_redis.set(job_key_prefix + j.job_id, b'invalid pickle data')

    with pytest.raises(DeserializationError, match='unable to deserialize job'):
        assert await j.info()


async def test_deserialize_job_raw():
    serialized = pickle.dumps({'f': 1, 'a': 2, 'k': 3, 't': 4, 'et': 5})
    assert deserialize_job_raw(serialized) == (1, 2, 3, 4, 5)
    with pytest.raises(DeserializationError, match='unable to deserialize job'):
        deserialize_job_raw(b'123')
