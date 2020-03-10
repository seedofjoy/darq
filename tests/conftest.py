import functools

import msgpack
import pytest
from aioredis import create_redis_pool
from arq.connections import ArqRedis

from darq import Darq
from darq.worker import create_worker
from . import redis_settings


@pytest.fixture(autouse=True)
async def auto_loop(loop):
    yield loop


@pytest.fixture
async def arq_redis():
    redis_ = await create_redis_pool(
        (redis_settings.host, redis_settings.port),
        encoding='utf8', commands_factory=ArqRedis,
    )
    await redis_.flushall()
    yield redis_
    redis_.close()
    await redis_.wait_closed()


@pytest.fixture
async def arq_redis_msgpack():
    commands_factory = functools.partial(
        ArqRedis,
        job_serializer=msgpack.packb,
        job_deserializer=functools.partial(msgpack.unpackb, raw=False),
    )
    redis_ = await create_redis_pool(
        (redis_settings.host, redis_settings.port),
        encoding='utf8', commands_factory=commands_factory,
    )
    await redis_.flushall()
    yield redis_
    redis_.close()
    await redis_.wait_closed()


@pytest.fixture
async def worker_factory(arq_redis):
    worker_ = None

    def create(darq, queue=None):
        nonlocal worker_
        worker_ = create_worker(darq, queue=queue)
        return worker_

    yield create

    if worker_:
        await worker_.close()


@pytest.fixture
async def darq():
    return Darq(redis_settings=redis_settings, burst=True, poll_delay=0)
