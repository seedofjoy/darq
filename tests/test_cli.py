from aiohttp.test_utils import loop_context
from click.testing import CliRunner

from darq import Darq
from darq.cli import cli
from . import redis_settings

darq = Darq(redis_settings=redis_settings, burst=True)


@darq.task
async def foobar(ctx):
    return 42


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert result.output.startswith('Usage: darq [OPTIONS] DARQ_APP\n')


def test_run():
    runner = CliRunner()
    with loop_context():
        result = runner.invoke(cli, ['tests.test_cli.darq'])
    assert result.exit_code == 0
    cli_output = 'Starting worker for 1 functions: tests.test_cli.foobar'
    assert cli_output in result.output


def test_check():
    runner = CliRunner()
    with loop_context():
        result = runner.invoke(cli, ['tests.test_cli.darq', '--check'])
    assert result.exit_code == 1
    expected = 'Health check failed: no health check sentinel value found'
    assert expected in result.output


async def mock_awatch():
    yield [1]


def test_run_watch(mocker):
    darq.redis_pool = None
    mocker.patch('watchgod.awatch', return_value=mock_awatch())
    runner = CliRunner()
    with loop_context():
        result = runner.invoke(cli, ['tests.test_cli.darq', '--watch', 'tests'])
    assert result.exit_code == 0
    assert '1 files changed, reloading darq worker...' in result.output
