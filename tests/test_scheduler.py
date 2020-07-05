import logging

import pytest

from darq import Darq
from darq.app import DarqException
from darq.connections import ArqRedis
from darq.cron import cron
from . import redis_settings
from .test_app import assert_worker_job_finished


async def foobar():
    return 42


async def foobar_custom():
    return 52


async def test_add_cron_jobs(
        darq, caplog, worker_factory, scheduler_factory, arq_redis,
        mocker,
):
    caplog.set_level(logging.INFO)

    with pytest.raises(DarqException):
        darq.add_cron_jobs(cron(foobar))

    mocker.patch('darq.scheduler.to_unix_ms', lambda x: 321)
    foobar_task = darq.task(foobar)
    darq.add_cron_jobs(
        cron('tests.test_scheduler.foobar', run_at_startup=True),
        cron(foobar, run_at_startup=True),
        cron(foobar, run_at_startup=True),  # duplicate doesn't start
        cron(foobar, name='custom_name', run_at_startup=True),
        cron(foobar_task, name='custom_name2', run_at_startup=True),
    )

    with pytest.raises(DarqException):
        # is not CronJob instance
        darq.add_cron_jobs(foobar_task)

    scheduler = scheduler_factory(darq)
    await scheduler.main()
    worker = worker_factory(darq)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 4
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    function_name = 'tests.test_scheduler.foobar'
    assert_worker_job_finished(
        records=caplog.records,
        job_id='cron:tests.test_scheduler.foobar:321',
        function_name=function_name,
        result='42',
    )
    assert_worker_job_finished(
        records=caplog.records,
        job_id='cron:foobar:321',
        function_name=function_name,
        result='42',
    )
    assert_worker_job_finished(
        records=caplog.records,
        job_id='custom_name:321',
        function_name=function_name,
        result='42',
    )
    assert_worker_job_finished(
        records=caplog.records,
        job_id='custom_name2:321',
        function_name=function_name,
        result='42',
    )


async def test_job_successful_on_specific_queue(
        darq, worker_factory, scheduler_factory, caplog, mocker,
):
    caplog.set_level(logging.INFO)

    foobar_task_default_queue = darq.task(foobar)
    foobar_task_custom_queue = darq.task(foobar_custom, queue='custom_queue')

    mocker.patch('darq.scheduler.to_unix_ms', lambda x: 3215)

    darq.add_cron_jobs(
        cron(foobar_task_default_queue, hour=1, run_at_startup=True),
        cron(foobar_task_custom_queue, hour=1, run_at_startup=True),
    )

    scheduler = scheduler_factory(darq)
    await scheduler.main()

    worker_default_queue = worker_factory(darq)
    await worker_default_queue.main()
    assert worker_default_queue.jobs_complete == 1
    assert worker_default_queue.jobs_failed == 0
    assert worker_default_queue.jobs_retried == 0
    assert_worker_job_finished(
        records=caplog.records,
        job_id='cron:foobar:3215',
        function_name='tests.test_scheduler.foobar',
        result='42',
    )

    worker_custom_queue = worker_factory(darq, queue_name='custom_queue')
    await worker_custom_queue.main()
    assert worker_custom_queue.jobs_complete == 1
    assert worker_custom_queue.jobs_failed == 0
    assert worker_custom_queue.jobs_retried == 0
    assert_worker_job_finished(
        records=caplog.records,
        job_id='cron:foobar_custom:3215',
        function_name='tests.test_scheduler.foobar_custom',
        result='52',
    )


async def test_not_run(darq, worker_factory, caplog, arq_redis):
    caplog.set_level(logging.INFO)

    foobar_task = darq.task(foobar)
    darq.add_cron_jobs(
        cron(foobar_task, hour=1, run_at_startup=False),
    )
    worker = worker_factory(darq)
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = '\n'.join(r.message for r in caplog.records)
    assert 'cron:foobar()' not in log


async def test_startup_shutdown(arq_redis, scheduler_factory):
    calls = []

    async def startup(scheduler_ctx):
        scheduler_ctx['test'] = 123
        assert isinstance(scheduler_ctx.get('redis'), ArqRedis)
        calls.append('startup')

    async def shutdown(scheduler_ctx):
        assert scheduler_ctx['test'] == 123
        assert isinstance(scheduler_ctx.get('redis'), ArqRedis)
        calls.append('shutdown')

    darq = Darq(
        redis_settings=redis_settings, burst=True,
        on_scheduler_startup=startup, on_scheduler_shutdown=shutdown,
    )
    foobar_task = darq.task(foobar)
    darq.add_cron_jobs(
        cron(foobar_task, hour=1),
    )

    scheduler = scheduler_factory(darq)
    await scheduler.main()
    await scheduler.close()

    assert calls == ['startup', 'shutdown']
