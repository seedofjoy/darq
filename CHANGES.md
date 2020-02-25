## Changelog

### dev (unpublished)
* **Breaking change**: Jobs no longer explicitly get `JobCtx` as the first argument, as in 99.9% cases it doesn't need it. To pass JobCtx simply add `_ctx` to the signature of the function, and it will be passed implicitly
* **Breaking change**: All cron jobs should be wrapped in `@task` decorator

### 0.0.3 (2020-02-25)
* `.delay()` now returns `arq_redis.enqueue_job` result (`Optional[Job]`)
* Add `py.typed` file
* Fixed `add_cron_jobs` typing

### 0.0.2 (2020-02-24)
* Add `add_cron_jobs` method

### 0.0.1 (2020-02-21)
First release
