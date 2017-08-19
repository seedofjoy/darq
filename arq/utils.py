"""
:mod:`utils`
============

Utilises for running arq used by other modules.
"""
import asyncio
import base64
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Tuple, Union

import aioredis
from aioredis.pool import RedisPool
from async_timeout import timeout

__all__ = ['RedisSettings', 'RedisMixin', 'next_cron']
logger = logging.getLogger('arq.utils')


class RedisSettings:
    """
    No-Op class used to hold redis connection redis_settings.
    """
    __slots__ = 'host', 'port', 'database', 'password', 'conn_retries', 'conn_timeout', 'conn_retry_delay'

    def __init__(self,
                 host='localhost',
                 port=6379,
                 database=0,
                 password=None,
                 conn_timeout=1,
                 conn_retries=5,
                 conn_retry_delay=1):
        """
        :param host: redis host
        :param port: redis port
        :param database: redis database id
        :param password: password for redis connection
        """
        self.host = host
        self.port = port
        self.database = database
        self.password = password
        self.conn_timeout = conn_timeout
        self.conn_retries = conn_retries
        self.conn_retry_delay = conn_retry_delay


class RedisMixin:
    """
    Mixin used to fined a redis pool and access it.
    """
    def __init__(self, *,
                 loop: asyncio.AbstractEventLoop=None,
                 redis_settings: RedisSettings=None,
                 existing_pool: RedisPool=None) -> None:
        """
        :param loop: asyncio loop to use for the redis pool
        :param redis_settings: connection settings to use for the pool
        :param existing_pool: existing pool, if set no new pool is created, instead this one is used
        """
        # the "or getattr(...) or" seems odd but it allows the mixin to work with subclasses which initialise
        # loop or redis_settings before calling super().__init__ and don't pass those parameters.
        self.loop = loop or getattr(self, 'loop', None) or asyncio.get_event_loop()
        self.redis_settings = redis_settings or getattr(self, 'redis_settings', None) or RedisSettings()
        self._redis_pool = existing_pool
        self._create_pool_lock = asyncio.Lock(loop=self.loop)

    async def create_redis_pool(self, *, _retry=0) -> RedisPool:
        """
        Create a new redis pool.
        """
        addr = self.redis_settings.host, self.redis_settings.port
        try:
            with timeout(self.redis_settings.conn_timeout):
                pool = await aioredis.create_pool(addr, loop=self.loop, db=self.redis_settings.database,
                                                  password=self.redis_settings.password)
        except (ConnectionError, OSError, aioredis.RedisError, asyncio.TimeoutError) as e:
            if _retry < self.redis_settings.conn_retries:
                logger.warning('redis connection error %s %s, %d retries remaining...',
                               e.__class__.__name__, e, self.redis_settings.conn_retries - _retry)
                await asyncio.sleep(self.redis_settings.conn_retry_delay)
                return await self.create_redis_pool(_retry=_retry + 1)
            else:
                raise
        else:
            if _retry > 0:
                logger.warning('redis connection successful')
            return pool

    async def get_redis_pool(self) -> RedisPool:
        """
        Get the redis pool, if a pool is already initialised it's returned, else one is crated.
        """
        async with self._create_pool_lock:
            if self._redis_pool is None:
                self._redis_pool = await self.create_redis_pool()
        return self._redis_pool

    async def get_redis_conn(self):
        """
        :return: redis connection context manager
        """
        pool = await self.get_redis_pool()
        return pool.get()

    async def log_redis_info(self, log_func):
        pool = await self.get_redis_pool()
        async with pool.get() as redis:
            info, key_count = await asyncio.gather(redis.info(), redis.dbsize())
            log_func(
                f'redis_version={info["server"]["redis_version"]} '
                f'mem_usage={info["memory"]["used_memory_human"]} '
                f'clients_connected={info["clients"]["connected_clients"]} '
                f'db_keys={key_count}'
            )

    async def close(self):
        """
        Close the pool and wait for all connections to close.
        """
        if self._redis_pool:
            self._redis_pool.close()
            await self._redis_pool.wait_closed()
            await self._redis_pool.clear()


def create_tz(utcoffset=0) -> timezone:
    """
    Create a python datetime.timezone with a given utc offset.

    :param utcoffset: utc offset in seconds, if 0 timezone.utc is returned.
    """
    if utcoffset == 0:
        return timezone.utc
    else:
        return timezone(timedelta(seconds=utcoffset))


EPOCH = datetime(1970, 1, 1)
EPOCH_TZ = EPOCH.replace(tzinfo=create_tz())


def timestamp() -> float:
    """
    This should be exactly the same as time.time(), we use this approach for consistency with
    other methods and possibly greater accuracy.
    :return: now in unix time, eg. seconds since 1970
    """
    return (datetime.utcnow() - EPOCH).total_seconds()


def to_unix_ms_tz(dt: datetime) -> Tuple[int, Union[int, None]]:
    """
    convert a datetime to number of milliseconds since 1970 and calculate timezone offset
    :param dt: datetime to evaluate
    :return: tuple - (unix time in milliseconds, utc offset in seconds)
    """
    utcoffset = dt.utcoffset()
    if utcoffset is not None:
        _utcoffset = utcoffset.total_seconds()
        unix = (dt - EPOCH_TZ).total_seconds() + _utcoffset
        return int(unix * 1000), int(_utcoffset)
    else:
        return int((dt - EPOCH).total_seconds() * 1000), None


def to_unix_ms(dt: datetime) -> int:
    """
    convert a datetime to number of milliseconds since 1970
    :param dt: datetime to evaluate
    :return: unix time in milliseconds
    """
    return to_unix_ms_tz(dt)[0]


def from_unix_ms(ms: int, utcoffset: int=None) -> datetime:
    """
    convert int to a datetime.

    :param ms: number of milliseconds since 1970
    :param utcoffset: if set a timezone i added to the datime based on the offset in seconds.
    :return: datetime - including timezone if utcoffset is not None, else timezone naïve
    """
    dt = EPOCH + timedelta(milliseconds=ms)
    if utcoffset is not None:
        dt = dt.replace(tzinfo=create_tz(utcoffset))
    return dt


def gen_random(length: int=20) -> bytes:
    """
    Create a random string.

    :param length: length of string to created, default 20
    """
    return base64.urlsafe_b64encode(os.urandom(length))[:length]


DEFAULT_CURTAIL = 80


def truncate(s: str, length: int=DEFAULT_CURTAIL) -> str:
    """
    Truncate a string and add an ellipsis (three dots) to the end if it was too long

    :param s: string to possibly truncate
    :param length: length to truncate the string to
    """
    if len(s) > length:
        s = s[:length - 1] + '…'
    return s


_dt_fields = [
    'month',
    'day',
    'weekday',
    'hour',
    'minute',
    'second',
    'microsecond',
]


def _get_next_dt(dt_, options):  # noqa: C901
    for field in _dt_fields:
        v = options[field]
        if v is None:
            continue
        if field == 'weekday':
            next_v = dt_.weekday()
        else:
            next_v = getattr(dt_, field)
        if isinstance(v, int):
            mismatch = next_v != v
        else:
            assert isinstance(v, (set, list, tuple))
            mismatch = next_v not in v
        # print(field, v, next_v, mismatch)
        if mismatch:
            micro = max(dt_.microsecond - options['microsecond'], 0)
            if field == 'month':
                if dt_.month == 12:
                    return datetime(dt_.year + 1, 1, 1)
                else:
                    return datetime(dt_.year, dt_.month + 1, 1)
            elif field in ('day', 'weekday'):
                return dt_ + timedelta(days=1) - timedelta(hours=dt_.hour, minutes=dt_.minute, seconds=dt_.second,
                                                           microseconds=micro)
            elif field == 'hour':
                return dt_ + timedelta(hours=1) - timedelta(minutes=dt_.minute, seconds=dt_.second, microseconds=micro)
            elif field == 'minute':
                return dt_ + timedelta(minutes=1) - timedelta(seconds=dt_.second, microseconds=micro)
            elif field == 'second':
                return dt_ + timedelta(seconds=1) - timedelta(microseconds=micro)
            else:
                assert field == 'microsecond'
                return dt_ + timedelta(microseconds=options['microsecond'] - dt_.microsecond)


def next_cron(preview_dt: datetime, *,
              month: Union[None, set, int]=None,
              day: Union[None, set, int]=None,
              weekday: Union[None, set, int, str]=None,
              hour: Union[None, set, int]=None,
              minute: Union[None, set, int]=None,
              second: Union[None, set, int]=0,
              microsecond: int=123456):
    """
    Find the next datetime matching the given parameters.
    """
    dt = preview_dt + timedelta(seconds=1)
    if isinstance(weekday, str):
        weekday = ['mon', 'tues', 'wed', 'thurs', 'fri', 'sat', 'sun'].index(weekday.lower())
    options = dict(
        month=month,
        day=day,
        weekday=weekday,
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond,
    )

    while True:
        next_dt = _get_next_dt(dt, options)
        # print(dt, next_dt)
        if next_dt is None:
            return dt
        dt = next_dt
