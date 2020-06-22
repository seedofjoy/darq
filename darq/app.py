import datetime
import importlib
import typing as t

from .connections import ArqRedis
from .connections import create_pool
from .connections import RedisSettings
from .constants import default_queue_name
from .cron import CronJob
from .jobs import Deserializer
from .jobs import Job
from .jobs import Serializer
from .registry import Registry
from .types import AnyCallable
from .types import AnyTimedelta
from .types import DataDict
from .types import JobEnqueueOptions
from .types import OnJobPostrunType
from .types import OnJobPrepublishType
from .types import OnJobPrerunType
from .utils import get_function_name
from .utils import SecondsTimedelta
from .worker import func as worker_func
from .worker import WorkerSettings

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

    :param queue_name: queue name to get jobs from
    :param redis_settings: settings for creating a redis connection
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
            *,
            queue_name: str = default_queue_name,
            redis_settings: t.Optional[RedisSettings] = None,
            burst: bool = False,
            on_startup: t.Callable[[CtxType], t.Awaitable[None]] = None,
            on_shutdown: t.Callable[[CtxType], t.Awaitable[None]] = None,
            on_job_prerun: t.Optional[OnJobPrerunType] = None,
            on_job_postrun: t.Optional[OnJobPostrunType] = None,
            on_job_prepublish: t.Optional[OnJobPrepublishType] = None,
            on_scheduler_startup: t.Callable[[], t.Awaitable[None]] = None,
            on_scheduler_shutdown: t.Callable[[], t.Awaitable[None]] = None,
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
        self.worker_settings = WorkerSettings(
            queue_name=queue_name, burst=burst, max_jobs=max_jobs,
            job_timeout=job_timeout, keep_result=keep_result,
            poll_delay=poll_delay, queue_read_limit=queue_read_limit,
            max_tries=max_tries, health_check_interval=health_check_interval,
            health_check_key=health_check_key, retry_jobs=retry_jobs,
            max_burst_jobs=max_burst_jobs, job_serializer=job_serializer,
            job_deserializer=job_deserializer,
        )
        self.redis_pool: 'ArqRedis' = None  # type: ignore
        self.redis_settings = redis_settings
        self.ctx = ctx
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.on_job_prerun = on_job_prerun
        self.on_job_postrun = on_job_postrun
        self.on_job_prepublish = on_job_prepublish
        self.on_scheduler_startup = on_scheduler_startup
        self.on_scheduler_shutdown = on_scheduler_shutdown
        self.default_job_expires = default_job_expires
        self.cron_jobs: t.List[CronJob] = []
        self.registry = Registry()

    async def connect(self, redis_pool: t.Optional[ArqRedis] = None) -> None:
        if self.redis_pool:
            return

        self.redis_pool = redis_pool or await create_pool(
            self.redis_settings,
            job_serializer=self.worker_settings.job_serializer,
            job_deserializer=self.worker_settings.job_deserializer,
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
        registered_tasks = {f.coroutine for f in self.registry.values()}
        for cj in cron_jobs:
            if not isinstance(cj, CronJob):
                raise DarqException(f'{cj!r} must be instance of CronJob')
            if cj.task not in registered_tasks:
                raise DarqException(
                    f'{cj.task!r} is not registered. '
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
        task_queue = queue
        task_expires = expires

        def _decorate(function: AnyCallable) -> AnyCallable:
            name = get_function_name(function)

            async def apply_async(
                    args: t.Optional[t.Sequence[t.Any]] = None,
                    kwargs: t.Optional[t.Mapping[str, t.Any]] = None,
                    *,
                    job_id: t.Optional[str] = None,
                    queue: t.Optional[str] = None,
                    defer_until: t.Optional[datetime.datetime] = None,
                    defer_by: t.Optional[AnyTimedelta] = None,
                    expires: t.Optional[AnyTimedelta] = None,
                    job_try: t.Optional[int] = None,
            ) -> t.Optional[Job]:
                """
                Enqueue a job with params.

                :param args: args to pass to the function
                :param kwargs: any keyword arguments to pass to the function
                :param job_id: ID of the job, can be used to enforce job
                               uniqueness
                :param queue: queue of the job, can be used to create job in
                              different queue
                :param defer_until: datetime at which to run the job
                :param defer_by: duration to wait before running the job
                :param expires: if the job still hasn't started after this
                                duration, do not run it
                :param job_try: useful when re-enqueueing jobs within a job
                :return: :class:`darq.jobs.Job` instance or ``None`` if a job
                         with this ID already exists
                """
                args = list(args) if args is not None else []
                kwargs = dict(kwargs) if kwargs is not None else {}
                queue = queue or task_queue
                expires = expires or task_expires or self.default_job_expires

                if not self.redis_pool:
                    raise DarqConnectionError(
                        'Darq app is not connected. Please, make '
                        '"await <darq_instance>.connect()" before calling '
                        'this function',
                    )
                metadata: DataDict = {}

                job_options = JobEnqueueOptions({
                    'job_id': job_id, 'queue_name': queue,
                    'defer_until': defer_until, 'defer_by': defer_by,
                    'expires': expires, 'job_try': job_try,
                })

                self.on_job_prepublish and await self.on_job_prepublish(
                    metadata, self.registry[name], args, kwargs, job_options,
                )
                if metadata:
                    kwargs['__metadata__'] = metadata
                return await self.redis_pool.enqueue_job(
                    name, tuple(args), kwargs, **job_options,
                )

            async def delay(*args: t.Any, **kwargs: t.Any) -> t.Optional[Job]:
                return await apply_async(args, kwargs)

            function.delay = delay  # type: ignore
            function.apply_async = apply_async  # type: ignore
            arq_function = worker_func(
                coroutine=function, name=name, default_queue=task_queue,
                keep_result=keep_result, timeout=timeout, max_tries=max_tries,
            )
            self.registry.add(arq_function)

            return function

        if func:
            return _decorate(func)

        return _decorate
