.. :changelog:

Changelog
---------

0.10.2(2022-02-03)
..................
* Add proper typing for functions wrapped with the @task decorator. Mypy will now check that parameters are passed correctly when calling ``func()`` and ``func.delay()``

0.10.1 (2021-07-29)
...................
* Add ``sentinel_timeout`` (defaults to 0.2) param to ``RedisSettings``

0.10.0 (2021-07-09)
...................
* **Breaking change**: Rename ``darq.worker.Function`` to ``darq.worker.Task``
* Made ``job`` to ``task`` naming migration
* Add max_jobs parameter to CLI (thanks to `@antonmyronyuk <https://github.com/antonmyronyuk>`_)
* Fixed bug with ``expires`` argument: ``default_job_expires`` could not be replaced with ``None`` in ``@task`` or ``.apply_async``

0.9.0 (2020-06-24)
..................
* **Breaking change**: Add ``scheduler_ctx`` param to ``on_scheduler_startup`` and ``on_scheduler_shutdown`` to share data between this callbacks. It already has ``ctx['redis']`` - instance of ``ArqRedis``

0.8.0 (2020-06-22)
..................
* **Breaking change**: Changed CLI command format. Before: ``darq some_project.darq_app.darq``. Now: ``darq -A some_project.darq_app.darq worker``
* **Breaking change**: Scheduler (cron jobs) now run's seperate from worker (see ``darq scheduler`` command)
* **Breaking change**: Changed some function signatures (rename arguments)
* **Breaking change**: Remove ``redis_pool`` param from ``Darq`` app
* Add ``on_scheduler_startup`` and ``on_scheduler_shutdown`` callbacks

0.7.2 (2020-06-18)
..................
* Fix some types (cron, OnJobPrepublishType)
* ``on_job_prerun`` now runs before "task started" log and ``on_job_postrun`` now runs after "task finished" log

0.7.1 (2020-05-25)
..................
* ``.apply_async``: Make ``args`` and ``kwargs`` arguments optional

0.7.0 (2020-05-25)
..................
* Fork ``arq`` to project and merge it with ``darq`` (It was easier to rewrite ``arq`` than to write a wrapper)
* **Breaking change**: Remove "magic" params from ``.delay``. For enqueue job with special params added ``.apply_async``.
* Add ``watch``-mode to CLI.
* Fix: Now worker will not run cronjob if it's functions queue not match with worker's

0.6.0 (2020-03-08)
..................
* **Breaking change**: Changed `Darq` constructor from single `config` param to separate params.
* `arq_function.coroutine` now has `.delay` method.

0.5.0 (2020-03-03)
..................
* Add ``on_job_prepublish(metadata, arq_function, args, kwargs)`` callback. ``metadata`` is mutable dict, which will be available at ``ctx['metadata']``.

0.4.0 (2020-03-03)
..................
* Add ``default_job_expires`` param to Darq (if the job still hasn't started after this duration, do not run it). Default - 1 day
* Add `expires` param to ``@task`` (if set - overwrites ``default_job_expires``)

0.3.1 (2020-03-02)
..................
* Rewrite warm shutdown: now during warm shutdown cron is disabled, on second signal the warm shutdown will be canceled

0.3.0 (2020-02-27)
..................
* **Breaking change**: ``on_job_prerun`` and ``on_job_postrun`` now accepts ``arq.worker.Function`` instead of the original function (it can still be accessed at ``arq_function.coroutine``)

0.2.1 (2020-02-26)
..................
* Fix ``add_cron_jobs`` method. Tests added.

0.2.0 (2020-02-26)
..................
* Add ``on_job_prerun(ctx, function, args, kwargs)`` and ``on_job_postrun(ctx, function, args, kwargs, result)`` callbacks.

0.1.0 (2020-02-26)
..................
* **Breaking change**: Jobs no longer explicitly get ``JobCtx`` as the first argument, as in 99.9% cases it doesn't need it. In future release will be possible to optionally pass ``JobCtx`` in some way.
* **Breaking change**: All cron jobs should be wrapped in ``@task`` decorator
* Directly pass ``functions`` to ``arq.Worker``, not names.

0.0.3 (2020-02-25)
..................
* ``.delay()`` now returns ``arq_redis.enqueue_job`` result (``Optional[Job]``)
* Add ``py.typed`` file
* Fixed ``add_cron_jobs`` typing

0.0.2 (2020-02-24)
..................
* Add ``add_cron_jobs`` method

0.0.1 (2020-02-21)
..................
First release
