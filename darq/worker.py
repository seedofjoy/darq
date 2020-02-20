import asyncio
import logging
import signal
import typing as t

from arq.worker import get_kwargs
from arq.worker import Worker as ArqWorker

logger = logging.getLogger('arq.worker')

SIGTERM_TIMEOUT = 30


class Worker(ArqWorker):

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


def create_worker(settings_cls: t.Any, **kwargs: t.Any) -> Worker:
    return Worker(**{**get_kwargs(settings_cls), **kwargs})  # type: ignore


def run_worker(settings_cls: t.Any, **kwargs: t.Any) -> Worker:
    worker = create_worker(settings_cls, **kwargs)
    worker.run()
    return worker
