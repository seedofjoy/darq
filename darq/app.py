import datetime
import functools
import importlib
import typing as t

import arq
from arq.connections import ArqRedis
from arq.connections import RedisSettings
from arq.cron import CronJob
from arq.jobs import Deserializer
from arq.jobs import Job
from arq.jobs import Serializer
from arq.utils import SecondsTimedelta

from .registry import Registry
from .types import AnyCallable
from .types import AnyTimedelta
from .types import DataDict
from .types import JobCtx
from .types import OnJobPostrunType
from .types import OnJobPrepublishType
from .types import OnJobPrerunType
from .utils import get_function_name

TD_1_DAY = datetime.timedelta(days=1)


class DarqException(Exception):
    pass


class DarqConnectionError(DarqException):
    pass


class DarqConfigError(DarqException):
    pass


class Darq:

    def __init__(
            self,
            redis_settings: t.Optional[RedisSettings] = None,
            redis_pool: t.Optional[ArqRedis] = None,
            burst: bool = False,
            on_startup: t.Callable[[JobCtx], t.Awaitable[None]] = None,
            on_shutdown: t.Callable[[JobCtx], t.Awaitable[None]] = None,
            max_jobs: int = 10,
            job_timeout: SecondsTimedelta = 300,
            keep_result: SecondsTimedelta = 3600,
            poll_delay: SecondsTimedelta = 0.5,
            queue_read_limit: t.Optional[int] = None,
            max_tries: int = 5,
            health_check_interval: SecondsTimedelta = 3600,
            health_check_key: t.Optional[str] = None,
            ctx: t.Optional[DataDict] = None,
            retry_jobs: bool = True,
            max_burst_jobs: int = -1,
            job_serializer: t.Optional[Serializer] = None,
            job_deserializer: t.Optional[Deserializer] = None,
            default_job_expires: AnyTimedelta = TD_1_DAY,
            on_job_prerun: t.Optional[OnJobPrerunType] = None,
            on_job_postrun: t.Optional[OnJobPostrunType] = None,
            on_job_prepublish: t.Optional[OnJobPrepublishType] = None,
    ) -> None:
        self.redis_settings = redis_settings
        self.redis_pool = redis_pool
        self.burst = burst
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.max_jobs = max_jobs
        self.job_timeout = job_timeout
        self.keep_result = keep_result
        self.poll_delay = poll_delay
        self.queue_read_limit = queue_read_limit
        self.max_tries = max_tries
        self.health_check_interval = health_check_interval
        self.health_check_key = health_check_key
        self.ctx = ctx
        self.retry_jobs = retry_jobs
        self.max_burst_jobs = max_burst_jobs
        self.job_serializer = job_serializer
        self.job_deserializer = job_deserializer

        self.cron_jobs: t.List[CronJob] = []
        self.default_job_expires = default_job_expires
        self.on_job_prerun = on_job_prerun
        self.on_job_postrun = on_job_postrun
        self.on_job_prepublish = on_job_prepublish
        self.registry = Registry()

    async def connect(self, redis_pool: t.Optional[ArqRedis] = None) -> None:
        if self.redis_pool:
            return

        self.redis_pool = (
            redis_pool or await arq.create_pool(self.redis_settings)
        )

    async def disconnect(self) -> None:
        if not self.redis_pool:
            return

        self.redis_pool.close()
        await self.redis_pool.wait_closed()

    def autodiscover_tasks(self, packages: t.Sequence[str]) -> None:
        for pkg in packages:
            importlib.import_module(pkg)

    def add_cron_jobs(self, *jobs: CronJob) -> None:
        for job in jobs:
            if not isinstance(job, CronJob):
                raise DarqException(f'{job!r} must be instance of CronJob')
            if job.coroutine not in self.registry.by_original_coro:
                raise DarqException(
                    f'{job.coroutine!r} is not registered. '
                    'Please, wrap it with @task decorator.',
                )
            # Replace original coroutine with wrapped by ``wrap_job_coroutine``
            arq_function = self.registry.by_original_coro[job.coroutine]
            job.coroutine = arq_function.coroutine  # type: ignore
            self.cron_jobs.append(job)

    def wrap_job_coroutine(
            self, function: t.Callable[..., t.Any],
    ) -> t.Callable[..., t.Any]:

        @functools.wraps(function)
        async def wrapper(ctx: JobCtx, *args: t.Any, **kwargs: t.Any) -> t.Any:
            arq_function = self.registry.by_original_coro[function]
            ctx['metadata'] = kwargs.pop('__metadata__', {})
            if self.on_job_prerun:
                await self.on_job_prerun(ctx, arq_function, args, kwargs)

            result = await function(*args, **kwargs)

            if self.on_job_postrun:
                await self.on_job_postrun(
                    ctx, arq_function, args, kwargs, result,
                )
            return result

        return wrapper

    def task(
            self,
            func: t.Optional[AnyCallable] = None,
            *,
            keep_result: t.Optional[AnyTimedelta] = None,
            timeout: t.Optional[AnyTimedelta] = None,
            max_tries: t.Optional[int] = None,
            queue: t.Optional[str] = None,
            expires: t.Optional[AnyTimedelta] = None,
    ) -> t.Any:

        def _decorate(function: AnyCallable) -> AnyCallable:
            name = get_function_name(function)

            async def delay(*args: t.Any, **kwargs: t.Any) -> t.Optional[Job]:
                if queue and '_queue_name' not in kwargs:
                    kwargs['_queue_name'] = queue
                if '_expires' not in kwargs:
                    kwargs['_expires'] = expires or self.default_job_expires

                if not self.redis_pool:
                    raise DarqConnectionError(
                        'Darq app is not connected. Please, make '
                        '"await <darq_instance>.connect()" before calling '
                        'this function',
                    )
                metadata: DataDict = {}

                arq_function = self.registry.by_original_coro[function]
                self.on_job_prepublish and await self.on_job_prepublish(
                    metadata, arq_function, args, kwargs,
                )
                if metadata:
                    kwargs['__metadata__'] = metadata
                return await self.redis_pool.enqueue_job(name, *args, **kwargs)

            function.delay = delay  # type: ignore
            arq_function = arq.worker.func(
                coroutine=self.wrap_job_coroutine(function), name=name,
                keep_result=keep_result, timeout=timeout, max_tries=max_tries,
            )
            self.registry.add(arq_function)

            return function

        if func:
            return _decorate(func)

        return _decorate
