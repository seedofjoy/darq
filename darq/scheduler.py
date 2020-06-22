import asyncio
import logging
import typing as t
from datetime import datetime

from .connections import create_pool
from .connections import RedisSettings
from .utils import poll
from .utils import to_seconds_strict
from .utils import to_unix_ms

if t.TYPE_CHECKING:  # pragma: no cover
    from darq import Darq
    from .connections import ArqRedis

log = logging.getLogger('darq.scheduler')


class Scheduler:
    """
    :param app: instance of :func:`darq.app.Darq`
    """

    def __init__(self, app: 'Darq') -> None:
        self.cron_jobs = app.cron_jobs
        assert self.cron_jobs and len(self.cron_jobs) > 0, \
            'at least one cron_job must be registered'

        settings = app.worker_settings
        self.app = app
        self.burst = settings.burst
        self.on_startup = app.on_scheduler_startup
        self.on_shutdown = app.on_scheduler_shutdown
        self.poll_delay_s = to_seconds_strict(settings.poll_delay)

        self.pool: 'ArqRedis' = None  # type: ignore
        self.redis_settings = app.redis_settings or RedisSettings()

        self.main_task: t.Optional[asyncio.Task[None]] = None
        self.loop = asyncio.get_event_loop()

    def run(self) -> None:
        """
        Sync function to run the scheduler,
        finally closes scheduler connections.
        """
        self.main_task = self.loop.create_task(self.main())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:  # pragma: no cover
            # happens on shutdown, fine
            pass
        finally:
            self.loop.run_until_complete(self.close())

    async def async_run(self) -> None:
        """
        Asynchronously run the scheduler, does not close connections.
        Useful when testing.
        """
        self.main_task = self.loop.create_task(self.main())
        await self.main_task

    async def main(self) -> None:
        log.info(
            'Starting cron scheduler for %d cron jobs: \n%s',
            len(self.cron_jobs), '\n'.join(str(cj) for cj in self.cron_jobs),
        )
        self.pool = await create_pool(self.redis_settings)
        await self.app.connect(self.pool)
        if self.on_startup:
            await self.on_startup()

        async for _ in poll(self.poll_delay_s):
            await self.run_cron()
            if self.burst:
                return

    async def run_cron(self) -> None:
        n = datetime.now()
        job_futures = set()

        for cron_job in self.cron_jobs:
            if cron_job.next_run is None:
                if cron_job.run_at_startup:
                    cron_job.next_run = n
                else:
                    cron_job.set_next(n)

            cron_job.next_run = t.cast(datetime, cron_job.next_run)

            if n >= cron_job.next_run:
                job_id = (
                    f'{cron_job.name}:{to_unix_ms(cron_job.next_run)}'
                    if cron_job.unique
                    else None
                )
                log.info(
                    'Scheduler: Sending due task %s (%s)',
                    cron_job.name, job_id,
                )
                job_futures.add(cron_job.task.apply_async(  # type: ignore
                    job_id=job_id,
                ))
                cron_job.set_next(n)

        job_futures and await asyncio.gather(*job_futures)

    async def close(self) -> None:
        if not self.pool:
            return
        if self.on_shutdown:
            await self.on_shutdown()
        await self.app.disconnect()
        self.pool.close()
        await self.pool.wait_closed()
        self.pool = None  # type: ignore

    def __repr__(self) -> str:
        return f'<Scheduler cron_jobs={len(self.cron_jobs)}>'


def create_scheduler(darq: 'Darq') -> Scheduler:
    return Scheduler(darq)


def run_scheduler(darq: 'Darq') -> Scheduler:
    scheduler = create_scheduler(darq)
    scheduler.run()
    return scheduler
