import re
from unittest.mock import patch

from aiohttp.test_utils import loop_context
from click.testing import CliRunner

from darq import Darq
from darq.cli import cli
from darq.cron import cron
from . import redis_settings

darq = Darq(redis_settings=redis_settings, burst=True)


@darq.task
async def foobar(ctx):
    return 42


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert result.output.startswith('Usage: darq [OPTIONS] COMMAND [ARGS]...\n')


def test_worker_run():
    runner = CliRunner()
    with loop_context():
        result = runner.invoke(cli, ['-A', 'tests.test_cli.darq', 'worker'])
    assert result.exit_code == 0
    cli_output = 'Starting worker for 1 functions: tests.test_cli.foobar'
    assert cli_output in result.output


def test_app_bad_argument():
    runner = CliRunner()
    with loop_context():
        result = runner.invoke(cli, ['-A', 'tests.test_cli.foobar', 'worker'])
    assert result.exit_code == 2
    cli_output_re = (
        r'Error: "APP" argument error\. <function foobar at .*> is not '
        r'instance of <class \'darq\.app\.Darq\'>'
    )
    assert re.search(cli_output_re, result.output)


def test_worker_check():
    runner = CliRunner()
    with loop_context():
        result = runner.invoke(cli, [
            '-A', 'tests.test_cli.darq', 'worker', '--check',
        ])
    assert result.exit_code == 1
    expected = 'Health check failed: no health check sentinel value found'
    assert expected in result.output


@patch('darq.worker.Worker')
def test_worker_queue(worker_mock):
    runner = CliRunner()
    custom_queue_name = 'my_queue'
    with loop_context():
        result = runner.invoke(cli, [
            '-A', 'tests.test_cli.darq', 'worker', '--queue', custom_queue_name,
        ])
    assert result.exit_code == 0
    worker_mock.assert_called_once_with(darq, queue_name=custom_queue_name)


@patch('darq.worker.Worker')
def test_worker_max_jobs(worker_mock):
    runner = CliRunner()
    max_jobs = 7
    with loop_context():
        result = runner.invoke(cli, [
            '-A', 'tests.test_cli.darq', 'worker', '--max-jobs', max_jobs,
        ])
    assert result.exit_code == 0
    worker_mock.assert_called_once_with(darq, max_jobs=max_jobs)


async def mock_awatch():
    yield [1]


def test_worker_run_watch(mocker):
    darq.redis_pool = None
    mocker.patch('watchgod.awatch', return_value=mock_awatch())
    runner = CliRunner()
    with loop_context():
        result = runner.invoke(cli, [
            '-A', 'tests.test_cli.darq', 'worker', '--watch', 'tests',
        ])
    assert result.exit_code == 0
    assert '1 files changed, reloading darq worker...' in result.output


async def mock_poll():
    yield


def test_scheduler_run(mocker):
    mocker.patch('darq.scheduler.poll', return_value=mock_awatch())
    runner = CliRunner()

    darq.add_cron_jobs(cron(foobar, hour=1))

    with loop_context():
        result = runner.invoke(cli, ['-A', 'tests.test_cli.darq', 'scheduler'])
    assert result.exit_code == 0
    cli_output = (
        'Starting cron scheduler for 1 cron jobs: \ntests.test_cli.foobar\n'
    )
    assert cli_output in result.output
