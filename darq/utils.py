import asyncio
import inspect
import typing as t
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from time import time
from typing import Any
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Union

from .types import AnyCallable


epoch = datetime(1970, 1, 1)
epoch_tz = epoch.replace(tzinfo=timezone.utc)
SecondsTimedelta = Union[int, float, timedelta]


def as_int(f: float) -> int:
    return int(round(f))


def timestamp_ms() -> int:
    return as_int(time() * 1000)


def to_unix_ms(dt: datetime) -> int:
    """
    convert a datetime to number of milliseconds since 1970 and calculate
    timezone offset
    """
    utcoffset = dt.utcoffset()
    ep = epoch if utcoffset is None else epoch_tz
    return as_int((dt - ep).total_seconds() * 1000)


def ms_to_datetime(unix_ms: int) -> datetime:
    return epoch + timedelta(seconds=unix_ms / 1000)


def to_ms(td: Optional[SecondsTimedelta]) -> Optional[int]:
    if td is None:
        return td
    elif isinstance(td, timedelta):
        td = td.total_seconds()
    return as_int(td * 1000)


def to_seconds_strict(td: SecondsTimedelta) -> float:
    if isinstance(td, timedelta):
        return td.total_seconds()
    return td


def to_seconds(td: Optional[SecondsTimedelta]) -> Optional[float]:
    if td is None:
        return td
    return to_seconds_strict(td)


DEFAULT_CURTAIL = 80


def truncate(s: str, length: int = DEFAULT_CURTAIL) -> str:
    """
    Truncate a string and add an ellipsis (three dots) to the end
    if it was too long

    :param s: string to possibly truncate
    :param length: length to truncate the string to
    """
    if len(s) > length:
        s = s[: length - 1] + '…'
    return s


def args_to_string(args: Sequence[Any], kwargs: Mapping[Any, Any]) -> str:
    arguments = ''
    if args:
        arguments = ', '.join(map(repr, args))
    if kwargs:
        if arguments:
            arguments += ', '
        arguments += ', '.join(f'{k}={v!r}' for k, v in sorted(kwargs.items()))
    return truncate(arguments)


def get_function_name(func: AnyCallable) -> str:
    module_str = func.__module__
    if module_str == '__main__':
        module = inspect.getmodule(func)
        if module and module.__spec__:
            module_str = module.__spec__.name

    return f'{module_str}.{func.__name__}'


async def poll(
        step: float, *, timeout: t.Optional[float] = None,
) -> t.AsyncIterator[float]:
    loop = asyncio.get_event_loop()
    start = loop.time()
    while True:
        before = loop.time()
        yield before - start
        after = loop.time()
        if timeout and after - start >= timeout:
            return
        wait = max([0, step - after + before])
        await asyncio.sleep(wait)
