import datetime
import importlib
import typing as t

import arq
from arq.connections import ArqRedis
from arq.connections import RedisSettings
from arq.jobs import Deserializer
from arq.jobs import Job
from arq.jobs import Serializer
from arq.utils import SecondsTimedelta

from .cron import CronJob
from .registry import Registry
from .types import AnyCallable
from .types import AnyTimedelta
from .types import DataDict
from .types import OnJobPostrunType
from .types import OnJobPrepublishType
from .types import OnJobPrerunType
from .utils import get_function_name

CtxType = t.Dict[t.Any, t.Any]
TD_1_DAY = datetime.timedelta(days=1)


class DarqException(Exception):
    pass


class DarqConnectionError(DarqException):
    pass


class DarqConfigError(DarqException):
    pass


class Darq:
    """
    Darq application.

    :param redis_settings: settings for creating a redis connection
    :param redis_pool: existing redis pool, generally None
    :param burst: whether to stop the worker once all jobs have been run
    :param on_startup: coroutine function to run at worker startup
    :param on_shutdown: coroutine function to run at worker shutdown
    :param on_job_prerun: coroutine function to run before job starts
    :param on_job_postrun: coroutine function to run after job finish
    :param on_job_prepublish: coroutine function to run before enqueue job
    :param max_jobs: maximum number of jobs to run at a time
    :param job_timeout: default job timeout (max run time)
    :param keep_result: default duration to keep job results for
    :param poll_delay: duration between polling the queue for new jobs
    :param queue_read_limit: the maximum number of jobs to pull from the queue
                             each time it's polled;
                             by default it equals ``max_jobs``
    :param max_tries: default maximum number of times to retry a job
    :param health_check_interval: how often to set the health check key
    :param health_check_key: redis key under which health check is set
    :param ctx: dict object, data from it will be pass to hooks:
                ``on_startup``, ``on_shutdown`` - can modify ``ctx``;
                ``on_job_prerun``, ``on_job_postrun`` - readonly
    :param retry_jobs: whether to retry jobs on Retry or CancelledError or not
    :param max_burst_jobs: the maximum number of jobs to process in burst mode
                           (disabled with negative values)
    :param job_serializer: a function that serializes Python objects to bytes,
                           defaults to pickle.dumps
    :param job_deserializer: a function that deserializes bytes into Python
                             objects, defaults to pickle.loads
    :param default_job_expires: default job expires. If the job still hasn't
                                started after this duration, do not run it
    """

    def __init__(
            self,
            redis_settings: t.Optional[RedisSettings] = None,
            redis_pool: t.Optional[ArqRedis] = None,
            burst: bool = False,
            on_startup: t.Callable[[CtxType], t.Awaitable[None]] = None,
            on_shutdown: t.Callable[[CtxType], t.Awaitable[None]] = None,
            on_job_prerun: t.Optional[OnJobPrerunType] = None,
            on_job_postrun: t.Optional[OnJobPostrunType] = None,
            on_job_prepublish: t.Optional[OnJobPrepublishType] = None,
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
    ) -> None:
        self.redis_settings = redis_settings
        self.redis_pool = redis_pool
        self.burst = burst
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.on_job_prerun = on_job_prerun
        self.on_job_postrun = on_job_postrun
        self.on_job_prepublish = on_job_prepublish
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
        self.default_job_expires = default_job_expires

        self.cron_jobs: t.List[CronJob] = []
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

    def add_cron_jobs(self, *cron_jobs: CronJob) -> None:
        """
        :param cron_jobs: list of cron jobs to run,
                          use :func:`darq.cron.cron` to create them
        """
        registered_coroutines = {f.coroutine for f in self.registry.values()}
        for cj in cron_jobs:
            if not isinstance(cj, CronJob):
                raise DarqException(f'{cj!r} must be instance of CronJob')
            if cj.coroutine not in registered_coroutines:
                raise DarqException(
                    f'{cj.coroutine!r} is not registered. '
                    'Please, wrap it with @task decorator.',
                )
            self.cron_jobs.append(cj)

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

                self.on_job_prepublish and await self.on_job_prepublish(
                    metadata, self.registry[name], args, kwargs,
                )
                if metadata:
                    kwargs['__metadata__'] = metadata
                return await self.redis_pool.enqueue_job(name, *args, **kwargs)

            function.delay = delay  # type: ignore
            arq_function = arq.worker.func(
                coroutine=function, name=name,
                keep_result=keep_result, timeout=timeout, max_tries=max_tries,
            )
            self.registry.add(arq_function)

            return function

        if func:
            return _decorate(func)

        return _decorate
