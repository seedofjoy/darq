import asyncio
import dataclasses
import logging
import signal
import typing as t
from datetime import datetime
from functools import partial
from signal import Signals
from time import time

import async_timeout
from aioredis import MultiExecError
from pydantic.utils import import_string

from .connections import create_pool
from .connections import log_redis_info
from .connections import RedisSettings
from .constants import default_queue_name
from .constants import health_check_key_suffix
from .constants import in_progress_key_prefix
from .constants import job_key_prefix
from .constants import result_key_prefix
from .constants import retry_key_prefix
from .jobs import deserialize_job_raw
from .jobs import Deserializer
from .jobs import JobResult
from .jobs import SerializationError
from .jobs import serialize_result
from .jobs import Serializer
from .utils import args_to_string
from .utils import ms_to_datetime
from .utils import poll
from .utils import SecondsTimedelta
from .utils import timestamp_ms
from .utils import to_ms
from .utils import to_seconds
from .utils import to_seconds_strict
from .utils import truncate

if t.TYPE_CHECKING:  # pragma: no cover
    from darq import Darq
    from .connections import ArqRedis

log = logging.getLogger('darq.worker')
no_result = object()

CoroutineType = t.Callable[..., t.Awaitable[t.Any]]
CtxType = t.Dict[t.Any, t.Any]


@dataclasses.dataclass
class Function:
    name: str
    coroutine: CoroutineType
    default_queue: t.Optional[str]
    timeout_s: t.Optional[float]
    keep_result_s: t.Optional[float]
    max_tries: t.Optional[int]


def func(
        coroutine: t.Union[str, Function, CoroutineType],
        *,
        name: t.Optional[str] = None,
        default_queue: t.Optional[str] = None,
        keep_result: t.Optional[SecondsTimedelta] = None,
        timeout: t.Optional[SecondsTimedelta] = None,
        max_tries: t.Optional[int] = None,
) -> Function:
    """
    Wrapper for a job function which lets you configure more settings.

    :param coroutine: coroutine function to call, can be a string to import
    :param name: name for function, if None, ``coroutine.__qualname__`` is used
    :param keep_result: duration to keep the result for, if 0 the result
                        is not kept
    :param timeout: maximum time the job should take
    :param max_tries: maximum number of tries allowed for the function,
                      use 1 to prevent retrying
    """
    if isinstance(coroutine, Function):
        return coroutine
    elif isinstance(coroutine, str):
        name = name or coroutine
        coroutine = import_string(coroutine)

    coroutine = t.cast(CoroutineType, coroutine)
    assert asyncio.iscoroutinefunction(coroutine), \
        f'{coroutine} is not a coroutine function'
    timeout = to_seconds(timeout)
    keep_result = to_seconds(keep_result)

    return Function(
        name=name or coroutine.__qualname__, coroutine=coroutine,
        default_queue=default_queue, timeout_s=timeout,
        keep_result_s=keep_result, max_tries=max_tries,
    )


class Retry(RuntimeError):
    """
    Special exception to retry the job (if ``max_retries`` hasn't been reached).

    :param defer: duration to wait before rerunning the job
    """

    def __init__(self, defer: t.Optional[SecondsTimedelta] = None) -> None:
        self.defer_score = to_ms(defer)

    def __repr__(self) -> str:
        return f'<Retry defer {(self.defer_score or 0) / 1000:0.2f}s>'

    def __str__(self) -> str:
        return repr(self)


class JobExecutionFailed(RuntimeError):
    def __eq__(self, other: t.Any) -> bool:
        if isinstance(other, JobExecutionFailed):
            return self.args == other.args
        return False


class FailedJobs(RuntimeError):
    def __init__(self, count: int, job_results: t.Sequence[JobResult]) -> None:
        self.count = count
        self.job_results = job_results

    def __str__(self) -> str:
        if self.count == 1 and self.job_results:
            exc = self.job_results[0].result
            return f'1 job failed {exc!r}'
        else:
            return (
                f'{self.count} jobs failed:\n'
                + '\n'.join(repr(r.result) for r in self.job_results)
            )

    def __repr__(self) -> str:
        return f'<{str(self)}>'


@dataclasses.dataclass
class WorkerSettings:
    """
    Worker settings

    :param queue_name: queue name to get jobs from
    :param burst: whether to stop the worker once all jobs have been run
    :param max_jobs: maximum number of jobs to run at a time
    :param job_timeout: default job timeout (max run time)
    :param keep_result: default duration to keep job results for
    :param poll_delay: duration between polling the queue for new jobs
    :param queue_read_limit: the maximum number of jobs to pull from the queue
                             each time it's polled;
                             by default it equals ``max_jobs``
    :param max_tries: default maximum number of times to retry a job
    :param health_check_interval: how often to set the health check key
    :param health_check_key: redis key under which health check is set
    :param retry_jobs: whether to retry jobs on Retry or CancelledError or not
    :param max_burst_jobs: the maximum number of jobs to process in burst mode
                           (disabled with negative values)
    :param job_serializer: a function that serializes Python objects to bytes,
                           defaults to pickle.dumps
    :param job_deserializer: a function that deserializes bytes into Python
                             objects, defaults to pickle.loads
    """
    queue_name: str = default_queue_name
    burst: bool = False
    max_jobs: int = 10
    job_timeout: SecondsTimedelta = 300
    keep_result: SecondsTimedelta = 3600
    poll_delay: SecondsTimedelta = 0.5
    queue_read_limit: t.Optional[int] = None
    max_tries: int = 5
    health_check_interval: SecondsTimedelta = 3600
    health_check_key: t.Optional[str] = None
    retry_jobs: bool = True
    max_burst_jobs: int = -1
    job_serializer: t.Optional[Serializer] = None
    job_deserializer: t.Optional[Deserializer] = None


class Worker:
    """
    Main class for running jobs.

    :param app: instance of :func:`darq.app.Darq`
    :param worker_settings: instance of :class:`darq.worker.WorkerSettings`
    """

    def __init__(
            self, app: 'Darq', **replace_kwargs: t.Dict[str, t.Any],
    ) -> None:
        settings = dataclasses.replace(app.worker_settings, **replace_kwargs)
        self.app = app
        self.functions: t.Dict[str, Function] = dict(app.registry)
        self.queue_name = settings.queue_name

        assert len(self.functions) > 0, \
            'at least one function or cron_job must be registered'
        self.burst = settings.burst
        self.on_startup = app.on_startup
        self.on_shutdown = app.on_shutdown
        self.on_job_prerun = app.on_job_prerun
        self.on_job_postrun = app.on_job_postrun
        self.sem = asyncio.BoundedSemaphore(settings.max_jobs)
        self.job_timeout_s = to_seconds_strict(settings.job_timeout)
        self.keep_result_s = to_seconds_strict(settings.keep_result)
        self.poll_delay_s = to_seconds_strict(settings.poll_delay)
        self.queue_read_limit = (
            settings.queue_read_limit or settings.max_jobs
        )
        self._queue_read_offset = 0
        self.max_tries = settings.max_tries
        self.health_check_interval = to_seconds_strict(
            settings.health_check_interval,
        )
        if settings.health_check_key is None:
            self.health_check_key = self.queue_name + health_check_key_suffix
        else:
            self.health_check_key = settings.health_check_key

        self.pool: 'ArqRedis' = None  # type: ignore
        self.redis_settings = app.redis_settings or RedisSettings()

        self.tasks: t.List[asyncio.Task[t.Any]] = []
        self.main_task: t.Optional[asyncio.Task[None]] = None
        self.graceful_shutdown_task: t.Optional[asyncio.Task[None]] = None
        self.graceful_shutdown_timeout = 30
        self.loop = asyncio.get_event_loop()
        self.ctx = app.ctx or {}
        max_timeout = max(
            f.timeout_s or self.job_timeout_s for f in self.functions.values()
        )
        self.in_progress_timeout_s = max_timeout + 10
        self.jobs_complete = 0
        self.jobs_retried = 0
        self.jobs_failed = 0
        self._last_health_check: float = 0
        self._last_health_check_log: t.Optional[str] = None
        self._add_signal_handler(signal.SIGINT, self.handle_sig)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.on_stop = None
        # whether or not to retry jobs on Retry and CancelledError
        self.retry_jobs = settings.retry_jobs
        self.max_burst_jobs = settings.max_burst_jobs
        self.job_serializer = settings.job_serializer
        self.job_deserializer = settings.job_deserializer

    def run(self) -> None:
        """
        Sync function to run the worker, finally closes worker connections.
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
        Asynchronously run the worker, does not close connections.
        Useful when testing.
        """
        self.main_task = self.loop.create_task(self.main())
        await self.main_task

    async def run_check(
            self,
            retry_jobs: t.Optional[bool] = None,
            max_burst_jobs: t.Optional[int] = None,
    ) -> int:
        """
        Run :func:`darq.worker.Worker.async_run`, check for failed jobs
        and raise :class:`darq.worker.FailedJobs` if any jobs have failed.

        :return: number of completed jobs
        """
        if retry_jobs is not None:
            self.retry_jobs = retry_jobs
        if max_burst_jobs is not None:
            self.max_burst_jobs = max_burst_jobs
        await self.async_run()
        if self.jobs_failed:
            failed_job_results = [
                r for r in await self.pool.all_job_results()
                if not r.success
            ]
            raise FailedJobs(self.jobs_failed, failed_job_results)
        else:
            return self.jobs_complete

    async def main(self) -> None:
        log.info(
            'Starting worker for %d functions: %s',
            len(self.functions), ', '.join(self.functions),
        )
        self.pool = await create_pool(self.redis_settings)
        await log_redis_info(self.pool, log.info)
        self.ctx['redis'] = self.pool
        await self.app.connect(self.pool)
        if self.on_startup:
            await self.on_startup(self.ctx)

        async for _ in poll(self.poll_delay_s):
            if self.graceful_shutdown_task:
                self.clean_tasks_done()
                continue

            await self._poll_iteration()

            if self.burst:
                if 0 <= self.max_burst_jobs <= self._jobs_started():
                    await asyncio.gather(*self.tasks)
                    return
                queued_jobs = await self.pool.zcard(self.queue_name)
                if queued_jobs == 0:
                    await asyncio.gather(*self.tasks)
                    return

    async def _poll_iteration(self) -> None:
        count = self.queue_read_limit
        if self.burst and self.max_burst_jobs >= 0:
            burst_jobs_remaining = self.max_burst_jobs - self._jobs_started()
            if burst_jobs_remaining < 1:
                return
            count = min(burst_jobs_remaining, count)

        async with self.sem:
            # don't bother with zrangebyscore until we have "space"
            # to run the jobs
            now = timestamp_ms()
            job_ids = await self.pool.zrangebyscore(
                self.queue_name, offset=self._queue_read_offset,
                count=count, max=now,
            )
        await self.run_jobs(job_ids)
        self.clean_tasks_done()
        await self.heart_beat()

    def clean_tasks_done(self) -> None:
        for task in self.tasks:
            if task.done():
                self.tasks.remove(task)
                # required to make sure errors in run_job get propagated
                task.result()

    async def run_jobs(self, job_ids: t.Sequence[str]) -> None:
        for job_id in job_ids:
            await self.sem.acquire()
            in_progress_key = in_progress_key_prefix + job_id
            with await self.pool as conn:
                _, _, ongoing_exists, score = await asyncio.gather(
                    conn.unwatch(),
                    conn.watch(in_progress_key),
                    conn.exists(in_progress_key),
                    conn.zscore(self.queue_name, job_id),
                )
                if ongoing_exists or not score:
                    # job already started elsewhere, or already finished and
                    # removed from queue
                    self.sem.release()
                    continue

                tr = conn.multi_exec()
                tr.setex(in_progress_key, self.in_progress_timeout_s, b'1')
                try:
                    await tr.execute()
                except MultiExecError:
                    # job already started elsewhere since we got 'existing'
                    self.sem.release()
                    log.debug(
                        'multi-exec error, job %s already started elsewhere',
                        job_id,
                    )
                    # https://github.com/samuelcolvin/arq/issues/131,
                    # avoid warnings in log
                    await asyncio.gather(*tr._results, return_exceptions=True)
                else:
                    task = self.loop.create_task(self.run_job(job_id, score))
                    task.add_done_callback(lambda _: self.sem.release())
                    self.tasks.append(task)

    async def run_job(self, job_id: str, score: float) -> None:
        start_ms = timestamp_ms()
        v, job_try, _ = await asyncio.gather(
            self.pool.get(job_key_prefix + job_id, encoding=None),
            self.pool.incr(retry_key_prefix + job_id),
            self.pool.expire(retry_key_prefix + job_id, 88400),
        )
        kwargs: t.Dict[str, t.Any]
        args: t.Sequence[t.Any]
        function_name, args, kwargs, enqueue_time_ms = '<unknown>', (), {}, 0

        async def job_failed(exc: Exception) -> None:
            self.jobs_failed += 1
            result_data_ = serialize_result(
                function=function_name,
                args=args,
                kwargs=kwargs,
                job_try=job_try,
                enqueue_time_ms=enqueue_time_ms,
                success=False,
                result=exc,
                start_ms=start_ms,
                finished_ms=timestamp_ms(),
                ref=f'{job_id}:{function_name}',
                serializer=self.job_serializer,
            )
            await asyncio.shield(self.abort_job(job_id, result_data_))

        if not v:
            log.warning('job %s expired', job_id)
            await job_failed(JobExecutionFailed('job expired'))
            return

        try:
            (
                function_name, args, kwargs, enqueue_job_try, enqueue_time_ms,
            ) = deserialize_job_raw(v, deserializer=self.job_deserializer)
        except SerializationError as e:
            log.exception('deserializing job %s failed', job_id)
            await job_failed(e)
            return

        try:
            function = self.functions[function_name]
        except KeyError:
            log.error('job %s, function %r not found', job_id, function_name)
            await job_failed(
                JobExecutionFailed(f'function {function_name!r} not found'),
            )
            return

        ref = f'{job_id}:{function_name}'

        if enqueue_job_try and enqueue_job_try > job_try:
            job_try = enqueue_job_try
            await self.pool.setex(
                retry_key_prefix + job_id, 88400, str(job_try),
            )

        max_tries = (
            self.max_tries
            if function.max_tries is None
            else function.max_tries
        )
        if job_try > max_tries:
            log.warning(
                '%6.2fs ! %s max retries %d exceeded',
                (timestamp_ms() - enqueue_time_ms) / 1000, ref, max_tries,
            )
            self.jobs_failed += 1
            result_data = serialize_result(
                function_name,
                args,
                kwargs,
                job_try,
                enqueue_time_ms,
                False,
                JobExecutionFailed(f'max {max_tries} retries exceeded'),
                start_ms,
                timestamp_ms(),
                ref,
                serializer=self.job_serializer,
            )
            return await asyncio.shield(self.abort_job(job_id, result_data))

        result = no_result
        exc_extra = None
        finish = False
        timeout_s = (
            self.job_timeout_s
            if function.timeout_s is None
            else function.timeout_s
        )
        incr_score: t.Optional[float] = None
        job_ctx = {
            'job_id': job_id,
            'job_try': job_try,
            'enqueue_time': ms_to_datetime(enqueue_time_ms),
            'score': score,
            'metadata': kwargs.pop('__metadata__', {}),
        }
        ctx = {**self.ctx, **job_ctx}
        start_ms = timestamp_ms()
        success = False
        try:
            if self.on_job_prerun:
                await self.on_job_prerun(ctx, function, args, kwargs)

            s = args_to_string(args, kwargs)
            extra = f' try={job_try}' if job_try > 1 else ''
            if (start_ms - score) > 1200:
                extra += f' delayed={(start_ms - score) / 1000:0.2f}s'
            log.info(
                '%6.2fs → %s(%s)%s',
                (start_ms - enqueue_time_ms) / 1000, ref, s, extra,
            )
            # run repr(result) and extra inside try/except as they can
            # raise exceptions
            try:
                async with async_timeout.timeout(timeout_s):
                    result = await function.coroutine(*args, **kwargs)
            except Exception as e:
                exc_extra = getattr(e, 'extra', None)
                if callable(exc_extra):
                    exc_extra = exc_extra()
                raise
            else:
                result_str = '' if result is None else truncate(repr(result))
        except Exception as e:
            finished_ms = timestamp_ms()
            t_ = (finished_ms - start_ms) / 1000
            if self.retry_jobs and isinstance(e, Retry):
                incr_score = e.defer_score
                log.info(
                    '%6.2fs ↻ %s retrying job in %0.2fs',
                    t_, ref, (e.defer_score or 0) / 1000,
                )
                if e.defer_score:
                    incr_score = e.defer_score + (timestamp_ms() - score)
                self.jobs_retried += 1
            elif self.retry_jobs and isinstance(e, asyncio.CancelledError):
                log.info('%6.2fs ↻ %s cancelled, will be run again', t_, ref)
                self.jobs_retried += 1
            else:
                log.exception(
                    '%6.2fs ! %s failed, %s: %s',
                    t_, ref, e.__class__.__name__, e,
                    extra={'extra': exc_extra},
                )
                result = e
                finish = True
                self.jobs_failed += 1
        else:
            success = True
            finished_ms = timestamp_ms()
            log.info(
                '%6.2fs ← %s ● %s',
                (finished_ms - start_ms) / 1000, ref, result_str,
            )
            finish = True
            self.jobs_complete += 1

        result_timeout_s = (
            self.keep_result_s
            if function.keep_result_s is None
            else function.keep_result_s
        )
        result_data = None
        if result is not no_result and result_timeout_s > 0:
            result_data = serialize_result(
                function_name,
                args,
                kwargs,
                job_try,
                enqueue_time_ms,
                success,
                result,
                start_ms,
                finished_ms,
                ref,
                serializer=self.job_serializer,
            )

        await asyncio.shield(self.finish_job(
            job_id, finish, result_data, result_timeout_s, incr_score,
        ))
        if self.on_job_postrun:
            await self.on_job_postrun(ctx, function, args, kwargs, result)

    async def finish_job(
            self, job_id: str, finish: bool, result_data: t.Optional[bytes],
            result_timeout_s: t.Optional[float], incr_score: t.Optional[float],
    ) -> None:
        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            delete_keys = [in_progress_key_prefix + job_id]
            if finish:
                if result_data:
                    tr.setex(
                        result_key_prefix + job_id, result_timeout_s,
                        result_data,
                    )
                delete_keys += [
                    retry_key_prefix + job_id, job_key_prefix + job_id,
                ]
                tr.zrem(self.queue_name, job_id)
            elif incr_score:
                tr.zincrby(self.queue_name, incr_score, job_id)
            tr.delete(*delete_keys)
            await tr.execute()

    async def abort_job(
            self, job_id: str, result_data: t.Optional[bytes],
    ) -> None:
        with await self.pool as conn:
            await conn.unwatch()
            tr = conn.multi_exec()
            tr.delete(
                retry_key_prefix + job_id,
                in_progress_key_prefix + job_id,
                job_key_prefix + job_id,
            )
            tr.zrem(self.queue_name, job_id)
            # result_data would only be None if serializing the result fails
            if (
                    result_data is not None
                    and self.keep_result_s > 0
            ):  # pragma: no branch
                tr.setex(
                    result_key_prefix + job_id, self.keep_result_s, result_data,
                )
            await tr.execute()

    async def heart_beat(self) -> None:
        await self.record_health()

    async def record_health(self) -> None:
        now_ts = time()
        if (now_ts - self._last_health_check) < self.health_check_interval:
            return
        self._last_health_check = now_ts
        pending_tasks = self.running_job_count()
        queued = await self.pool.zcard(self.queue_name)
        info = (
            f'{datetime.now():%b-%d %H:%M:%S} j_complete={self.jobs_complete} '
            f'j_failed={self.jobs_failed} j_retried={self.jobs_retried} '
            f'j_ongoing={pending_tasks} queued={queued}'
        )
        await self.pool.setex(
            self.health_check_key,
            self.health_check_interval + 1,
            info.encode(),
        )
        log_suffix = info[info.index('j_complete='):]
        if (
                self._last_health_check_log
                and log_suffix != self._last_health_check_log
        ):
            log.info('recording health: %s', info)
            self._last_health_check_log = log_suffix
        elif not self._last_health_check_log:
            self._last_health_check_log = log_suffix

    def _add_signal_handler(
            self, signal: signal.Signals, handler: t.Callable[[int], None],
    ) -> None:
        self.loop.add_signal_handler(signal, partial(handler, signal))

    def _jobs_started(self) -> int:
        return (
            self.jobs_complete
            + self.jobs_retried
            + self.jobs_failed
            + len(self.tasks)
        )

    def running_job_count(self) -> int:
        return sum(not task.done() for task in self.tasks)

    async def graceful_shutdown(self, signum: int) -> None:
        running_job_count = self.running_job_count()
        if running_job_count:
            log.info(
                'Warm shutdown. Awaiting for %d jobs with %d seconds timeout.',
                running_job_count, self.graceful_shutdown_timeout,
            )
            async for _ in poll(0.1, timeout=self.graceful_shutdown_timeout):
                if not self.running_job_count():
                    break
        self.shutdown(signum)

    def shutdown(self, signum: int) -> None:
        sig = Signals(signum)
        log.info(
            'shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ '
            '%d ongoing to cancel',
            sig.name,
            self.jobs_complete,
            self.jobs_failed,
            self.jobs_retried,
            len(self.tasks),
        )
        for task in self.tasks:
            if not task.done():
                task.cancel()
        self.main_task and self.main_task.cancel()
        self.on_stop and self.on_stop(sig)

    def handle_sig(self, signum: int) -> None:
        if self.graceful_shutdown_task:
            self.graceful_shutdown_task.cancel()
            self.shutdown(signum)
        else:
            self.graceful_shutdown_task = self.loop.create_task(
                self.graceful_shutdown(signum),
            )

    async def close(self) -> None:
        if not self.pool:
            return
        await asyncio.gather(*self.tasks)
        await self.pool.delete(self.health_check_key)
        if self.on_shutdown:
            await self.on_shutdown(self.ctx)
        await self.app.disconnect()
        self.pool.close()
        await self.pool.wait_closed()
        self.pool = None  # type: ignore

    def __repr__(self) -> str:
        return (
            f'<Worker j_complete={self.jobs_complete} '
            f'j_failed={self.jobs_failed} j_retried={self.jobs_retried} '
            f'j_ongoing={self.running_job_count()}>'
        )


async def async_check_health(
        redis_settings: t.Optional[RedisSettings],
        health_check_key: t.Optional[str] = None,
        queue: t.Optional[str] = None,
) -> int:
    redis_settings = redis_settings or RedisSettings()
    redis = await create_pool(redis_settings)
    queue = queue or default_queue_name
    health_check_key = health_check_key or (queue + health_check_key_suffix)

    data = await redis.get(health_check_key)
    if not data:
        log.warning('Health check failed: no health check sentinel value found')
        result = 1
    else:
        log.info('Health check successful: %s', data)
        result = 0
    redis.close()
    await redis.wait_closed()
    return result


def check_health(darq: 'Darq', queue: str) -> int:
    """
    Run a health check on the worker and return the appropriate exit code.
    :return: 0 if successful, 1 if not
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        async_check_health(
            darq.redis_settings, darq.worker_settings.health_check_key, queue,
        ),
    )


def create_worker(
        darq: 'Darq', **overwrite_settings: t.Dict[str, t.Any],
) -> Worker:
    return Worker(darq, **overwrite_settings)


def run_worker(
        darq: 'Darq', **overwrite_settings: t.Dict[str, t.Any],
) -> Worker:
    worker = create_worker(darq, **overwrite_settings)
    worker.run()
    return worker
