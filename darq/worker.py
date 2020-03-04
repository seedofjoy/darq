import asyncio
import logging
import typing as t

from arq.worker import async_check_health
from arq.worker import Function  # noqa: F401  need for reimport
from arq.worker import Worker as ArqWorker

from .app import Darq
from .types import DataDict
from .types import JobCtx
from .utils import poll

logger = logging.getLogger('arq.worker')


class Worker(ArqWorker):

    def __init__(self, app: Darq, queue: str) -> None:
        self.app = app
        self.warm_shutdown_timeout = 30
        self.warm_shutdown_task: t.Optional[asyncio.Task[None]] = None

        async def wrapped_on_startup(ctx: JobCtx) -> None:
            await self.app.connect(ctx['redis'])
            self.app.on_startup and await self.app.on_startup(ctx)

        async def wrapped_on_shutdown(ctx: JobCtx) -> None:
            self.app.on_shutdown and await self.app.on_shutdown(ctx)
            await self.app.disconnect()

        extra_kwargs: DataDict = {
            'on_startup': wrapped_on_startup,
            'on_shutdown': wrapped_on_shutdown,
        }
        if queue:
            extra_kwargs['queue_name'] = queue

        super().__init__(
            functions=app.registry.get_functions(),
            redis_settings=app.redis_settings,
            redis_pool=app.redis_pool,
            burst=app.burst,
            max_jobs=app.max_jobs,
            job_timeout=app.job_timeout,
            keep_result=app.keep_result,
            poll_delay=app.poll_delay,
            queue_read_limit=app.queue_read_limit,
            max_tries=app.max_tries,
            health_check_interval=app.health_check_interval,
            health_check_key=app.health_check_key,
            ctx=app.ctx,
            retry_jobs=app.retry_jobs,
            max_burst_jobs=app.max_burst_jobs,
            job_serializer=app.job_serializer,
            job_deserializer=app.job_deserializer,
            cron_jobs=app.cron_jobs,
            **extra_kwargs,
        )

    def has_running_tasks(self) -> bool:
        return any(not task.done() for task in self.tasks)

    async def run_jobs(self, job_ids: t.Sequence[str]) -> None:
        if self.warm_shutdown_task:
            return
        await super().run_jobs(job_ids)  # type: ignore

    async def run_cron(self) -> None:
        if self.warm_shutdown_task:
            return
        await super().run_cron()  # type: ignore

    def handle_sig(self, signum: int) -> None:
        if self.warm_shutdown_task:
            self.warm_shutdown_task.cancel()
            super().handle_sig(signum)  # type: ignore
        else:
            self.warm_shutdown_task = self.loop.create_task(
                self.warm_shutdown(signum),
            )

    async def warm_shutdown(self, signum: int) -> None:
        if self.has_running_tasks():
            awaiting_task_count = sum(not task.done() for task in self.tasks)
            logger.info(
                'Warm shutdown. Awaiting for %d jobs with %d seconds timeout.',
                awaiting_task_count, self.warm_shutdown_timeout,
            )
            async for _ in poll(step=0.1, timeout=self.warm_shutdown_timeout):
                if not self.has_running_tasks():
                    break

        super().handle_sig(signum)  # type: ignore


def create_worker(darq: Darq, queue: str) -> Worker:
    return Worker(darq, queue)


def run_worker(darq: Darq, queue: str) -> Worker:
    worker = create_worker(darq, queue)
    worker.run()
    return worker


def check_health(darq: Darq, queue: str) -> int:
    """
    Run a health check on the worker and return the appropriate exit code.
    :return: 0 if successful, 1 if not
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        async_check_health(darq.redis_settings, darq.health_check_key, queue),
    )
