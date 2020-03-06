import asyncio
import logging
import signal
from unittest.mock import MagicMock

import pytest

from darq import Darq
from . import redis_settings


async def foobar():
    pass


@pytest.mark.asyncio
async def test_handle_sig_delayed(caplog, arq_redis, worker_factory):
    caplog.set_level(logging.INFO)

    darq = Darq(redis_settings=redis_settings)
    darq.task(foobar)

    worker = await worker_factory(darq)

    long_running_task_mock = MagicMock(done=MagicMock(return_value=False))
    worker.main_task = MagicMock()
    worker.tasks = [
        MagicMock(done=MagicMock(return_value=True)),
        long_running_task_mock,
    ]

    assert len(caplog.records) == 0
    worker.handle_sig(signal.SIGINT)
    await asyncio.sleep(0)
    assert len(caplog.records) == 1
    assert caplog.records[0].message == (
        'Warm shutdown. Awaiting for 1 jobs with 30 seconds timeout.'
    )
    assert worker.main_task.cancel.call_count == 0
    assert worker.tasks[0].cancel.call_count == 0
    assert worker.tasks[1].cancel.call_count == 0
    await asyncio.sleep(0)

    worker.tasks[1].done.return_value = True
    await asyncio.sleep(1)

    assert 'shutdown on SIGINT' in caplog.records[1].message
    assert worker.main_task.cancel.call_count == 1
    assert worker.tasks[0].cancel.call_count == 0
    assert worker.tasks[1].cancel.call_count == 0


@pytest.mark.asyncio
async def test_handle_sig_hard(caplog, worker_factory):
    caplog.set_level(logging.INFO)

    darq = Darq(redis_settings=redis_settings)
    darq.task(foobar)

    worker = await worker_factory(darq)

    long_running_task_mock = MagicMock(done=MagicMock(return_value=False))
    worker.main_task = MagicMock()
    worker.tasks = [long_running_task_mock]

    assert len(caplog.records) == 0
    worker.handle_sig(signal.SIGINT)
    await asyncio.sleep(0)
    assert len(caplog.records) == 1
    assert caplog.records[0].message == (
        'Warm shutdown. Awaiting for 1 jobs with 30 seconds timeout.'
    )
    worker.handle_sig(signal.SIGINT)
    assert caplog.records[1].message == (
        'shutdown on SIGINT ◆ 0 jobs complete ◆ 0 failed ◆ 0 retries ◆ '
        '1 ongoing to cancel'
    )
