import logging
from typing import Any

import darq
from aiohttp_example import signals

log = logging.getLogger(__name__)


async def startup_services(ctx: dict[str, Any]) -> None:
    ctx['_services'] = []
    for init_async_gen in signals.get_cleanup_ctx_factories():
        async_gen = init_async_gen(None)
        await async_gen.__anext__()
        ctx['_services'].append(async_gen)


async def shutdown_services(ctx: dict[str, Any]) -> None:
    for async_gen in reversed(ctx['_services']):
        try:
            await async_gen.__anext__()
        except StopAsyncIteration:
            pass


async def on_worker_startup(ctx: dict[str, Any]) -> None:
    logging.basicConfig(level=logging.INFO)
    await startup_services(ctx)


async def on_worker_shutdown(ctx: dict[str, Any]) -> None:
    await shutdown_services(ctx)

darq = darq.Darq(
    redis_settings=darq.RedisSettings(host='redis'),
    on_startup=on_worker_startup,
    on_shutdown=on_worker_shutdown,
)

darq.autodiscover_tasks([
    'aiohttp_example.apps.say_hello.tasks',
])
