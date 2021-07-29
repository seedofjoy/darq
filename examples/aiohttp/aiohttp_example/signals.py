import logging
from typing import Any, AsyncIterator, Callable

import aioredis

from aiohttp_example.darq import darq
from aiohttp_example.db import create_engine
from aiohttp_example.services import services

log = logging.getLogger(__name__)


async def connect_darq(*args: Any) -> AsyncIterator[None]:
    await darq.connect()
    yield
    await darq.disconnect()


async def connect_db(*args: Any) -> AsyncIterator[None]:
    db = await create_engine()
    services.db = db
    log.info('Connected to dummy database')

    yield
    db.close()
    await db.wait_closed()
    log.info('Disconnected from dummy database')


async def connect_redis(*args: Any) -> AsyncIterator[None]:
    redis_client = await aioredis.create_redis_pool('redis://redis')
    services.redis = redis_client
    log.info(f'Connected to redis (db={redis_client.db})')

    yield
    redis_client.close()
    await redis_client.wait_closed()
    log.info('Disconnected from redis')


def get_cleanup_ctx_factories(
) -> list[Callable[..., AsyncIterator[None]]]:
    return [
        connect_db,
        connect_redis,
    ]
