import pytest
from aioredis import create_redis_pool
from arq.connections import ArqRedis

from darq.worker import create_worker
from . import redis_settings


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
async def worker_factory():
    worker_ = None

    async def create(darq, queue=None):
        nonlocal worker_
        worker_ = create_worker(darq, queue=queue)
        return worker_

    yield create

    if worker_:
        await worker_.close()
