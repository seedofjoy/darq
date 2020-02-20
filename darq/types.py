import datetime
import sys
import typing as t

import arq

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

AnyCallable = t.Callable[..., t.Any]


class JobCtx(TypedDict):
    redis: arq.ArqRedis
    job_id: str
    job_try: int
    enqueue_time: datetime.datetime
    score: int
