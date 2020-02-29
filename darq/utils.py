import asyncio
import inspect
import typing as t

from .types import AnyCallable


def get_function_name(func: AnyCallable) -> str:
    module_str = func.__module__
    if module_str == '__main__':
        module = inspect.getmodule(func)
        if module and module.__spec__:
            module_str = module.__spec__.name

    return f'{module_str}.{func.__name__}'


async def poll(step: float, timeout: float) -> t.AsyncIterator[float]:
    loop = asyncio.get_event_loop()
    start = loop.time()
    while True:
        before = loop.time()
        yield before - start
        after = loop.time()
        if after - start >= timeout:
            return
        wait = max([0, step - after + before])
        await asyncio.sleep(wait)
