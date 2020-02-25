import datetime
import importlib
import typing as t

import arq
from arq.connections import ArqRedis
from arq.cron import CronJob
from arq.jobs import Job

from .registry import Registry
from .types import AnyCallable
from .utils import get_function_name


class DarqException(Exception):
    pass


class DarqConnectionError(DarqException):
    pass


class DarqConfigError(DarqException):
    pass


class Darq:

    def __init__(self, config: t.Dict[str, t.Any]) -> None:
        self.registry = Registry()
        self.config = config.copy()
        if 'functions' in self.config:
            raise DarqConfigError(
                '"functions" should not exist in config, all functions will '
                'be collected automatically. Just wrap your functions with '
                '@darq.task decorator.',
            )
        if 'queue_name' in self.config:
            raise DarqConfigError(
                '"queue_name" should not exist in config. '
                'To specify queue in worker - use "-Q" arg in cli.',
            )
        if not self.config.get('cron_jobs'):
            self.config['cron_jobs'] = []
        if not isinstance(self.config['cron_jobs'], list):
            self.config['cron_jobs'] = list(self.config['cron_jobs'])

        self.redis: t.Optional[arq.ArqRedis] = None
        if config.get('redis_pool'):
            self.redis = config['redis_pool']
        self.connected = bool(self.redis)

    async def connect(self, redis_pool: t.Optional[ArqRedis] = None) -> None:
        if self.connected:
            return

        if redis_pool:
            self.redis = redis_pool
        else:
            self.redis = await arq.create_pool(self.config['redis_settings'])
        self.connected = True

    async def disconnect(self) -> None:
        if not self.connected:
            return

        if self.redis:
            self.redis.close()
            await self.redis.wait_closed()
        self.connected = False

    def autodiscover_tasks(self, packages: t.Sequence[str]) -> None:
        for pkg in packages:
            importlib.import_module(pkg)

    def add_cron_jobs(self, *jobs: CronJob) -> None:
        for job in jobs:
            if not isinstance(job, CronJob):
                raise ValueError(f'{job!r} must be instance of CronJob')
        self.config['cron_jobs'].extend(jobs)

    def task(
            self,
            func: t.Optional[AnyCallable] = None,
            *,
            keep_result: t.Union[int, float, datetime.timedelta, None] = None,
            timeout: t.Union[int, float, datetime.timedelta, None] = None,
            max_tries: t.Optional[int] = None,
            queue: t.Optional[str] = None,
    ) -> t.Any:

        def _decorate(function: AnyCallable) -> AnyCallable:
            name = get_function_name(function)
            worker_func = arq.worker.func(
                coroutine=function, name=name, keep_result=keep_result,
                timeout=timeout, max_tries=max_tries,
            )
            self.registry.add(worker_func)

            async def delay(*args: t.Any, **kwargs: t.Any) -> t.Optional[Job]:
                if queue:
                    kwargs['_queue_name'] = queue
                if not self.connected or not self.redis:
                    raise DarqConnectionError(
                        'Darq app is not connected. Please, make '
                        '"await <darq_instance>.connect()" before calling '
                        'this function',
                    )
                return await self.redis.enqueue_job(name, *args, **kwargs)

            function.delay = delay  # type: ignore
            return function

        if func:
            return _decorate(func)

        return _decorate
