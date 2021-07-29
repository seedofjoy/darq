from aioredis import Redis

from aiohttp_example.db import DummyDbEngine


class Services:
    db: DummyDbEngine
    redis: Redis


services = Services()
