# darq

![Lint & test](https://github.com/seedofjoy/darq/workflows/Lint%20&%20test/badge.svg?branch=master)

A small wrapper around arq

## Features
* Celery-like `@task` decorator, adds `.delay()` to enqueue job
* Graceful shutdown: waits until running tasks are finished

## Quick start

```python
# some_project/darq_app.py
import asyncio
import darq

darq = darq.Darq({'redis_settings': darq.RedisSettings(host='redis')})


@darq.task
async def add_to_42(a: int) -> int:
    return 42 + a


async def main():
    # Before adding tasks to queue we should connect darq instance to redis
    await darq.connect()
    
    # Direct call job as function:
    result = await add_to_42(5)  # result == 47

    # Celery-like add task to queue:
    await add_to_42.delay(a=5)

    await darq.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
```

And start worker:
```sh
python3 -m darq.cli some_project.darq_app.darq
```

Worker output:
```
15:24:42: Starting worker for 1 functions: some_project.darq_app.add_to_42
15:24:42: redis_version=5.0.7 mem_usage=834.87K clients_connected=1 db_keys=2
15:25:08:   0.22s → 1315f27608e9408392bf5d3310bca38c:darq_app.add_to_42(a=5)
15:25:08:   0.00s ← 1315f27608e9408392bf5d3310bca38c:darq_app.add_to_42 ● 47
```
