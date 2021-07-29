from aiohttp_example.services import services


async def get_visited_count(name: str) -> int:
    visited_count_raw = await services.redis.hget('user_visited_count', name)
    return int(visited_count_raw) if visited_count_raw else 0


async def incr_visited_count(name: str) -> None:
    await services.redis.hincrby('user_visited_count', name)
