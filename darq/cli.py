import logging.config
import os
import sys

import click
from arq.logs import default_log_config
from pydantic.utils import import_string

from .app import Darq
from .worker import check_health
from .worker import run_worker

health_check_help = 'Health Check: run a health check and exit.'
verbose_help = 'Enable verbose output.'


@click.command('darq')
@click.argument('darq-app', type=str, required=True)
@click.option('--check', is_flag=True, help=health_check_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
@click.option('-Q', '--queue', type=str, default=None)
def cli(*, darq_app: str, check: bool, verbose: bool, queue: str) -> None:
    """
    Job queues in python with asyncio and redis.
    CLI to run the darq worker.

    DARQ_APP - path to Darq app instance.
    For example: someproject.darq.darq_app
    """
    sys.path.append(os.getcwd())
    darq = import_string(darq_app)
    if not isinstance(darq, Darq):
        raise click.BadArgumentUsage(
            f'DARQ_APP argument error. {darq!r} is not instance of {Darq!r}',
        )

    logging.config.dictConfig(default_log_config(verbose))

    if check:
        exit(check_health(darq))
    else:
        run_worker(darq, queue)


if __name__ == '__main__':
    cli()
