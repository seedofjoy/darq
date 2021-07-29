import logging

from aiohttp_example.darq import darq
from .lib import get_visited_count

log = logging.getLogger(__name__)


@darq.task
async def say_hello_from_worker(name: str) -> None:
    visited_count = await get_visited_count(name)
    log.info(
        'Hello from worker, %s. Visited count: %d',
        name, visited_count,
    )
