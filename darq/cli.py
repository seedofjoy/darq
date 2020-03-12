import asyncio
import logging.config
import os
import sys
import typing as t
from signal import Signals

import click
from pydantic.utils import import_string

from .app import Darq
from .logs import default_log_config
from .version import VERSION
from .worker import check_health
from .worker import create_worker
from .worker import run_worker

health_check_help = 'Health Check: run a health check and exit.'
watch_help = 'Watch a directory and reload the worker upon changes.'
verbose_help = 'Enable verbose output.'


@click.command('darq')
@click.version_option(VERSION, '-V', '--version', prog_name='darq')
@click.argument('darq-app', type=str, required=True)
@click.option('--check', is_flag=True, help=health_check_help)
@click.option(
    '--watch', type=click.Path(exists=True, dir_okay=True, file_okay=False),
    help=watch_help,
)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
@click.option('-Q', '--queue', type=str, default=None)
def cli(
        *, darq_app: str, check: bool, watch: str, verbose: bool, queue: str,
) -> None:
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

    overwrite_settings: t.Dict[str, t.Any] = {}
    if queue is not None:
        overwrite_settings['queue_name'] = queue

    if check:
        exit(check_health(darq, queue))
    else:
        if watch:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(
                watch_reload(watch, darq, loop, **overwrite_settings),
            )
        else:
            run_worker(darq, **overwrite_settings)


async def watch_reload(
        path: str, darq: Darq, loop: asyncio.AbstractEventLoop,
        **overwrite_settings: t.Dict[str, t.Any],
) -> None:
    try:
        from watchgod import awatch
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'watchgod not installed, use `pip install watchgod`',
        ) from e

    stop_event = asyncio.Event()
    worker = create_worker(darq, **overwrite_settings)

    tasks: t.List[asyncio.Task[t.Any]] = []

    def cancel_tasks() -> None:
        while tasks:
            tasks.pop().cancel()

    try:
        worker.on_stop = (
            lambda s: s != Signals.SIGUSR1 and stop_event.set()  # type: ignore
        )
        tasks.append(loop.create_task(worker.async_run()))
        async for changes in awatch(path, stop_event=stop_event):
            click.echo(
                f'{len(changes)} files changed, reloading darq worker...',
            )
            worker.handle_sig(Signals.SIGUSR1)
            await worker.close()
            cancel_tasks()
            tasks.append(loop.create_task(worker.async_run()))
        cancel_tasks()
    finally:
        await worker.close()


if __name__ == '__main__':
    cli()
