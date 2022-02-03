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
from .scheduler import run_scheduler
from .version import __version__
from .worker import check_health
from .worker import create_worker
from .worker import run_worker

health_check_help = 'Health Check: run a health check and exit.'
watch_help = 'Watch a directory and reload the worker upon changes.'
verbose_help = 'Enable verbose output.'


class ContextObject(t.NamedTuple):
    darq: Darq


@click.group('darq')
@click.pass_context
@click.version_option(__version__, '-V', '--version', prog_name='darq')
@click.option('-A', '--app', type=str, required=True)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
def cli(ctx: click.Context, *, app: str, verbose: bool) -> None:
    """
    Job queues in python with Asyncio and Redis.

    CLI to run the Darq worker.

    app - path to Darq app instance.
    For example: someproject.darq.darq_app
    """
    sys.path.append(os.getcwd())
    darq = import_string(app)
    if not isinstance(darq, Darq):
        raise click.BadArgumentUsage(
            f'"APP" argument error. {darq!r} is not instance of {Darq!r}',
        )

    ctx.obj = ContextObject(darq)
    logging.config.dictConfig(default_log_config(verbose))


@cli.command()
@click.pass_obj
@click.option('-Q', '--queue', type=str, default=None)
@click.option('--max-jobs', type=int, default=None)
@click.option(
    '--watch', type=click.Path(exists=True, dir_okay=True, file_okay=False),
    help=watch_help,
)
@click.option('--check', is_flag=True, help=health_check_help)
def worker(
        ctx_obj: ContextObject,
        *,
        queue: str,
        max_jobs: int,
        watch: str,
        check: bool,
) -> None:
    """
    CLI to run the Darq worker.
    """
    darq = ctx_obj.darq

    overwrite_settings: t.Dict[str, t.Any] = {}
    if queue is not None:
        overwrite_settings['queue_name'] = queue
    if max_jobs is not None:
        overwrite_settings['max_jobs'] = max_jobs

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


@cli.command()
@click.pass_obj
def scheduler(ctx_obj: ContextObject) -> None:
    """
    CLI to run the scheduler (cron jobs)
    """
    darq = ctx_obj.darq
    run_scheduler(darq)


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

    def on_stop(sig: Signals) -> None:
        if sig != Signals.SIGUSR1:
            stop_event.set()

    try:
        worker.on_stop = on_stop
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


if __name__ == '__main__':  # pragma: no cover
    cli()
