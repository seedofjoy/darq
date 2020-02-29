import asyncio
import logging
import typing as t

from arq.worker import async_check_health
from arq.worker import Function  # noqa: F401  need for reimport
from arq.worker import get_kwargs
from arq.worker import Worker as ArqWorker

from .app import Darq
from .types import JobCtx
from .utils import poll

logger = logging.getLogger('arq.worker')


class Worker(ArqWorker):

    def __init__(
            self, darq_app: Darq, queue: str,
            *args: t.Any, **kwargs: t.Any,
    ) -> None:
        self.darq_app = darq_app
        settings = self.get_settings(queue)

        self.warm_shutdown_timeout = 30
        self.warm_shutdown_task: t.Optional[asyncio.Task[None]] = None

        super().__init__(*args, **{**settings, **kwargs})

    def get_settings(self, queue: str) -> t.Dict[str, t.Any]:
        settings = {**self.darq_app.config}
        settings['functions'] = self.darq_app.registry.get_functions()
        if queue:
            settings['queue_name'] = queue

        on_startup = settings.get('on_startup')

        async def wrapped_on_startup(ctx: JobCtx) -> None:
            await self.darq_app.connect(ctx['redis'])
            if on_startup:
                await on_startup(ctx)

        on_shutdown = settings.get('on_shutdown')

        async def wrapped_on_shutdown(ctx: JobCtx) -> None:
            if on_shutdown:
                await on_shutdown(ctx)
            await self.darq_app.disconnect()

        settings['on_startup'] = wrapped_on_startup
        settings['on_shutdown'] = wrapped_on_shutdown
        return get_kwargs(settings)  # type: ignore

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


def create_worker(darq: Darq, queue: str, **kwargs: t.Any) -> Worker:
    return Worker(darq, queue, **kwargs)


def run_worker(darq: Darq, queue: str, **kwargs: t.Any) -> Worker:
    worker = create_worker(darq, queue, **kwargs)
    worker.run()
    return worker


def check_health(darq: Darq) -> int:
    """
    Run a health check on the worker and return the appropriate exit code.
    :return: 0 if successful, 1 if not
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        async_check_health(
            darq.config.get('redis_settings'),
            darq.config.get('health_check_key'),
            darq.config.get('queue_name'),
        ),
    )
