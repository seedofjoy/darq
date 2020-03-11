import logging

from arq.worker import Function  # noqa: F401  need for reimport
from arq.worker import Worker as ArqWorker

from .app import Darq
from .types import DataDict

log = logging.getLogger('darq.worker')


class Worker(ArqWorker):

    def __init__(self, app: Darq, queue: str) -> None:
        self.app = app

        extra_kwargs: DataDict = {}
        if queue:
            extra_kwargs['queue_name'] = queue

        super().__init__(
            app=app,
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
            on_startup=app.on_startup,
            on_shutdown=app.on_shutdown,
            on_job_prerun=app.on_job_prerun,
            on_job_postrun=app.on_job_postrun,
            **extra_kwargs,
        )


def create_worker(darq: Darq, queue: str) -> Worker:
    return Worker(darq, queue)


def run_worker(darq: Darq, queue: str) -> Worker:
    worker = create_worker(darq, queue)
    worker.run()
    return worker
