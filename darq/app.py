import datetime
import importlib
import typing as t

import arq
from arq.constants import default_queue_name

from .registry import Registry
from .types import AnyCallable
from .utils import get_function_name


class DarqException(Exception):
    pass


class DarqConnectionError(DarqException):
    pass


class Darq:

    def __init__(self, config: t.Dict[str, t.Any]) -> None:
        self.registry = Registry()
        self.config = config
        self.redis: t.Optional[arq.ArqRedis] = None
        self.connected = False

    async def connect(self) -> None:
        self.redis = await arq.create_pool(self.config['redis_settings'])
        self.connected = True

    async def disconnect(self) -> None:
        if not self.connected:
            return

        if self.redis:
            self.redis.close()
            await self.redis.wait_closed()
        self.connected = False

    def autodiscover_tasks(self, packages: t.Sequence[str]) -> None:
        for pkg in packages:
            importlib.import_module(pkg)

    def get_worker_settings(
            self, queue: t.Optional[str] = None,
    ) -> t.Dict[str, t.Any]:
        return {
            **self.config,
            **{
                'functions': self.registry.get_function_names(),
                'queue_name': queue or default_queue_name,
            },
        }

    def task(
            self,
            func: t.Optional[AnyCallable] = None,
            *,
            keep_result: t.Union[int, float, datetime.timedelta, None] = None,
            timeout: t.Union[int, float, datetime.timedelta, None] = None,
            max_tries: t.Optional[int] = None,
            queue: t.Optional[str] = None,
    ) -> t.Any:

        def _decorate(function: AnyCallable) -> AnyCallable:
            name = get_function_name(function)
            worker_func = arq.worker.func(
                coroutine=function, name=name, keep_result=keep_result,
                timeout=timeout, max_tries=max_tries,
            )
            self.registry.add(worker_func)

            async def delay(*args: t.Any, **kwargs: t.Any) -> None:
                if queue:
                    kwargs['_queue_name'] = queue
                if not self.connected or not self.redis:
                    raise DarqConnectionError(
                        'Darq app is not connected. Please, make '
                        '"await <darq_instance>.connect()" before calling '
                        'this function',
                    )
                await self.redis.enqueue_job(name, *args, **kwargs)

            function.delay = delay  # type: ignore
            return function

        if func:
            return _decorate(func)

        return _decorate
