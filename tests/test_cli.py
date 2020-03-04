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
    result = runner.invoke(cli, ['tests.test_cli.darq'])
    assert result.exit_code == 0
    cli_output = 'Starting worker for 1 functions: tests.test_cli.foobar'
    assert cli_output in result.output
