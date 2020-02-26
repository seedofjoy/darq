import asyncio
import logging
import signal
import typing as t

from arq.worker import async_check_health
from arq.worker import get_kwargs
from arq.worker import Worker as ArqWorker

from .app import Darq
from .types import JobCtx

logger = logging.getLogger('arq.worker')

SIGTERM_TIMEOUT = 30


class Worker(ArqWorker):

    def __init__(
            self, darq_app: Darq, queue: str,
            *args: t.Any, **kwargs: t.Any,
    ) -> None:
        self.darq_app = darq_app
        settings = self.get_settings(queue)
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

    def handle_sig(self, signum: int) -> None:
        if signum == signal.SIGTERM and self.has_running_tasks():
            self.main_task and self.main_task.cancel()
            awaiting_task_count = sum(not task.done() for task in self.tasks)
            logger.info(
                'Awaiting for %d jobs with timeout %d',
                awaiting_task_count, SIGTERM_TIMEOUT,
            )
            asyncio.ensure_future(
                self.handle_sig_delayed(signum, timeout=SIGTERM_TIMEOUT),
            )
        else:
            super().handle_sig(signum)  # type: ignore

    async def handle_sig_delayed(
            self, signum: int, *, timeout: t.Union[int, float],
    ) -> None:
        elapsed = 0
        while elapsed <= timeout:
            if not self.has_running_tasks():
                break
            await asyncio.sleep(1)
            elapsed += 1
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
