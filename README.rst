darq
====

[ ~ Dependencies scanned by PyUp.io ~ ]

.. image:: https://github.com/seedofjoy/darq/workflows/Lint%20&%20test/badge.svg?branch=master
   :target: https://github.com/seedofjoy/darq/actions

.. image:: https://codecov.io/gh/seedofjoy/darq/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/seedofjoy/darq

|

Async task manager with Celery-like features. Fork of `arq <http://github.com/samuelcolvin/arq>`_.


Features
--------
* Celery-like ``@task`` decorator, adds ``.delay()`` to enqueue job
* Proper ``mypy`` type checking: all arguments passed to ``.delay()`` will be checked against the original function signature
* Graceful shutdown: waits until running tasks are finished


Installation
------------
Darq uses ``aioredis`` 1.x as Redis client. Unfortunately, this library has been abandoned, and does not support Python 3.11. I made a fork with compatability fixes: ``evo-aioredis`` (https://github.com/evo-company/aioredis-py).

Because of this, ``aioredis`` is not currently added as Darq dependency, and you must install it yourself:

For Python<3.11 you can use:

.. code:: shell

   pip install aioredis<2.0.0

For Python 3.11 (and older versions too) you can use fork:

.. code:: shell

   pip install evo-aioredis<2.0.0


Quick start
-----------

.. code:: python

    # some_project/darq_app.py
    import asyncio
    import darq

    darq = darq.Darq(redis_settings=darq.RedisSettings(host='redis'))


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


And start worker:

.. code:: shell

    python3 -m darq.cli -A some_project.darq_app.darq worker


Worker output:

.. code:: shell

    15:24:42: Starting worker for 1 functions: some_project.darq_app.add_to_42
    15:24:42: redis_version=5.0.7 mem_usage=834.87K clients_connected=1 db_keys=2
    15:25:08:   0.22s → 1315f27608e9408392bf5d3310bca38c:darq_app.add_to_42(a=5)
    15:25:08:   0.00s ← 1315f27608e9408392bf5d3310bca38c:darq_app.add_to_42 ● 47
