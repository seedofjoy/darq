import asyncio
import functools
import logging
import typing as t
from dataclasses import dataclass
from datetime import datetime, timedelta
from operator import attrgetter
from ssl import SSLContext
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from uuid import uuid4

import aioredis
from aioredis import MultiExecError, Redis

from .constants import default_queue_name
from .constants import job_key_prefix
from .constants import result_key_prefix
from .jobs import deserialize_job
from .jobs import Deserializer
from .jobs import Job
from .jobs import JobDef
from .jobs import JobResult
from .jobs import serialize_job
from .jobs import Serializer
from .utils import timestamp_ms
from .utils import to_ms
from .utils import to_unix_ms

log = logging.getLogger('darq.connections')


@dataclass
class RedisSettings:
    """
    No-Op class used to hold redis connection redis_settings.

    Used by :func:`darq.connections.create_pool`
    and :class:`darq.worker.Worker`.
    """

    host: Union[str, List[Tuple[str, int]]] = 'localhost'
    port: int = 6379
    database: int = 0
    password: Optional[str] = None
    ssl: Union[bool, None, SSLContext] = None
    conn_timeout: int = 1
    conn_retries: int = 5
    conn_retry_delay: int = 1

    sentinel: bool = False
    sentinel_master: str = 'mymaster'

    def __repr__(self) -> str:
        return '<RedisSettings {}>'.format(
            ' '.join(f'{k}={v}' for k, v in self.__dict__.items()),
        )


# extra time after the job is expected to start when the job key should expire,
# 1 day in ms
expires_extra_ms = 86_400_000


class ArqRedis(Redis):
    """
    Thin subclass of ``aioredis.Redis`` which adds
    :func:`darq.connections.enqueue_job`.

    :param redis_settings: an instance of ``darq.connections.RedisSettings``.
    :param job_serializer: a function that serializes Python objects to bytes,
                           defaults to pickle.dumps
    :param job_deserializer: a function that deserializes bytes into Python
                             objects, defaults to pickle.loads
    :param kwargs: keyword arguments directly passed to ``aioredis.Redis``.
    """

    def __init__(
        self,
        pool_or_conn: Union[aioredis.ConnectionsPool, aioredis.RedisConnection],
        job_serializer: Optional[Serializer] = None,
        job_deserializer: Optional[Deserializer] = None,
        **kwargs: Dict[str, t.Any],
    ) -> None:
        self.job_serializer = job_serializer
        self.job_deserializer = job_deserializer
        super().__init__(pool_or_conn, **kwargs)

    async def enqueue_job(
        self,
        function: str,
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
        *,
        job_id: Optional[str] = None,
        queue_name: Optional[str] = None,
        defer_until: Optional[datetime] = None,
        defer_by: Union[None, int, float, timedelta] = None,
        expires: Union[None, int, float, timedelta] = None,
        job_try: Optional[int] = None,
    ) -> Optional[Job]:
        """
        Enqueue a job.

        :param function: Name of the function to call
        :param args: args to pass to the function
        :param kwargs: kwargs to pass to the function
        :param job_id: ID of the job, can be used to enforce job uniqueness
        :param queue_name: queue of the job, can be used to create job in
                           different queue
        :param defer_until: datetime at which to run the job
        :param defer_by: duration to wait before running the job
        :param expires: if the job still hasn't started after this duration,
                        do not run it
        :param job_try: useful when re-enqueueing jobs within a job
        :return: :class:`darq.jobs.Job` instance or ``None`` if a job with this
                 ID already exists
        """
        job_id = job_id or uuid4().hex
        job_key = job_key_prefix + job_id
        queue_name = queue_name or default_queue_name

        assert not (defer_until and defer_by), \
            "use either 'defer_until' or 'defer_by' or neither, not both"

        defer_by_ms = to_ms(defer_by)
        expires_ms = to_ms(expires)

        with await self as conn:
            pipe = conn.pipeline()
            pipe.unwatch()
            pipe.watch(job_key)
            job_exists = pipe.exists(job_key)
            job_result_exists = pipe.exists(result_key_prefix + job_id)
            await pipe.execute()
            if await job_exists or await job_result_exists:
                return None

            enqueue_time_ms = timestamp_ms()
            if defer_until is not None:
                score = to_unix_ms(defer_until)
            elif defer_by_ms:
                score = enqueue_time_ms + defer_by_ms
            else:
                score = enqueue_time_ms

            expires_ms = (
                expires_ms or score - enqueue_time_ms + expires_extra_ms
            )

            job = serialize_job(
                function, args, kwargs, job_try, enqueue_time_ms,
                serializer=self.job_serializer,
            )
            tr = conn.multi_exec()
            tr.psetex(job_key, expires_ms, job)
            tr.zadd(queue_name, score, job_id)
            try:
                await tr.execute()
            except MultiExecError:
                # job got enqueued since we checked 'job_exists'
                # https://github.com/samuelcolvin/arq/issues/131
                # avoid warnings in log
                await asyncio.gather(*tr._results, return_exceptions=True)
                return None
        return Job(
            job_id, redis=self, _queue_name=queue_name,
            _deserializer=self.job_deserializer,
        )

    async def _get_job_result(self, key: str) -> JobResult:
        job_id = key[len(result_key_prefix):]
        job = Job(job_id, self, _deserializer=self.job_deserializer)
        r = t.cast(JobResult, await job.result_info())
        r.job_id = job_id
        return r

    async def all_job_results(self) -> List[JobResult]:
        """
        Get results for all jobs in redis.
        """
        keys = await self.keys(result_key_prefix + '*')
        results = await asyncio.gather(*[self._get_job_result(k) for k in keys])
        return sorted(results, key=attrgetter('enqueue_time'))

    async def _get_job_def(self, job_id: str, score: int) -> JobDef:
        v = await self.get(job_key_prefix + job_id, encoding=None)
        jd = deserialize_job(v, deserializer=self.job_deserializer)
        jd.score = score
        return jd

    async def queued_jobs(
            self, *, queue_name: str = default_queue_name,
    ) -> List[JobDef]:
        """
        Get information about queued, mostly useful when testing.
        """
        jobs = await self.zrange(queue_name, withscores=True)
        return await asyncio.gather(*[
            self._get_job_def(job_id, score) for job_id, score in jobs
        ])


async def create_pool(
    settings: RedisSettings = None,
    *,
    retry: int = 0,
    job_serializer: Optional[Serializer] = None,
    job_deserializer: Optional[Deserializer] = None,
) -> ArqRedis:
    """
    Create a new redis pool, retrying up to ``conn_retries`` times if the
    connection fails.

    Similar to ``aioredis.create_redis_pool`` except it returns a
    :class:`darq.connections.ArqRedis` instance, thus allowing job enqueuing.
    """
    settings = settings or RedisSettings()
    addr: t.Union[
        Tuple[str, int],
        List[Tuple[str, int]],
    ]

    if type(settings.host) is str and settings.sentinel:
        raise ValueError(
            "str provided for 'host' but 'sentinel' is true; "
            'list of sentinels expected',
        )

    if settings.sentinel:
        addr = t.cast(List[Tuple[str, int]], settings.host)

        async def pool_factory(
                *args: Any, **kwargs: Any,
        ) -> aioredis.sentinel.pool.SentinelPool:
            client = await aioredis.sentinel.create_sentinel_pool(
                *args, ssl=t.cast(RedisSettings, settings).ssl, **kwargs,
            )
            return client.master_for(
                t.cast(RedisSettings, settings).sentinel_master,
            )

    else:
        pool_factory = functools.partial(
            aioredis.create_pool,
            create_connection_timeout=settings.conn_timeout, ssl=settings.ssl,
        )
        addr = t.cast(str, settings.host), settings.port

    try:
        pool = await pool_factory(
            addr, db=settings.database, password=settings.password,
            encoding='utf8',
        )
        pool = ArqRedis(
            pool,
            job_serializer=job_serializer, job_deserializer=job_deserializer,
        )
    except (OSError, aioredis.RedisError, asyncio.TimeoutError) as e:
        if retry < settings.conn_retries:
            log.warning(
                'redis connection error %s %s %s, %d retries remaining...',
                addr,
                e.__class__.__name__,
                e,
                settings.conn_retries - retry,
            )
            await asyncio.sleep(settings.conn_retry_delay)
        else:
            raise
    else:
        if retry > 0:
            log.info('redis connection successful')
        return pool

    # recursively attempt to create the pool outside the except block to avoid
    # "During handling of the above exception..." madness
    return await create_pool(
        settings, retry=retry + 1,
        job_serializer=job_serializer, job_deserializer=job_deserializer,
    )


async def log_redis_info(
        redis: ArqRedis, log_func: t.Callable[[str], None],
) -> None:
    with await redis as r:
        info, key_count = await asyncio.gather(r.info(), r.dbsize())
    log_func(
        f'redis_version={info["server"]["redis_version"]} '
        f'mem_usage={info["memory"]["used_memory_human"]} '
        f'clients_connected={info["clients"]["connected_clients"]} '
        f'db_keys={key_count}',
    )
