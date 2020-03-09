## Changelog

### 0.7.0 (2020.03-09)
* Fork `arq` to project. It will be easier to rewrite `arq` than to write wrapper
* Add `watch`-mode to CLI.
* Move `cron` from `arq` to `darq`, with types and tests.

### 0.6.0 (2020-03-08)
* **Breaking change**: Changed `Darq` constructor from single `config` param to separate params.
* `arq_function.coroutine` now has `.delay` method.

### 0.5.0 (2020-03-03)
* Add `on_job_prepublish(metadata, arq_function, args, kwargs)` callback. `metadata` is mutable dict, which will be available at `ctx['metadata']`.

### 0.4.0 (2020-03-03)
* Add `default_job_expires` param to Darq (if the job still hasn't started after this duration, do not run it). Default - 1 day
* Add `expires` param to `@task` (if set - overwrites `default_job_expires`)

### 0.3.1 (2020-03-02)
* Rewrite warm shutdown: now during warm shutdown cron is disabled, on second signal the warm shutdown will be canceled

### 0.3.0 (2020-02-27)
* **Breaking change**: `on_job_prerun` and `on_job_postrun` now accepts `arq.worker.Function` instead of the original function (it can still be accessed at `arq_function.coroutine`)

### 0.2.1 (2020-02-26)
* Fix `add_cron_jobs` method. Tests added.

### 0.2.0 (2020-02-26)
* Add `on_job_prerun(ctx, function, args, kwargs)` and `on_job_postrun(ctx, function, args, kwargs, result)` callbacks.

### 0.1.0 (2020-02-26)
* **Breaking change**: Jobs no longer explicitly get `JobCtx` as the first argument, as in 99.9% cases it doesn't need it. In future release will be possible to optionally pass JobCtx in some way.
* **Breaking change**: All cron jobs should be wrapped in `@task` decorator
* Directly pass `functions` to `arq.Worker`, not names.

### 0.0.3 (2020-02-25)
* `.delay()` now returns `arq_redis.enqueue_job` result (`Optional[Job]`)
* Add `py.typed` file
* Fixed `add_cron_jobs` typing

### 0.0.2 (2020-02-24)
* Add `add_cron_jobs` method

### 0.0.1 (2020-02-21)
First release
