import datetime
import sys
import typing as t

import arq

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

AnyCallable = t.Callable[..., t.Any]
AnyTimedelta = t.Union[int, float, datetime.timedelta]
DataDict = t.Dict[str, t.Any]
ArgsType = t.Sequence[t.Any]
KwargsType = t.Mapping[str, t.Any]


class JobCtx(TypedDict):
    redis: arq.ArqRedis
    job_id: str
    job_try: int
    enqueue_time: datetime.datetime
    score: int
    metadata: DataDict


OnJobPrerunType = t.Callable[
    [JobCtx, arq.worker.Function, ArgsType, KwargsType],
    t.Awaitable[None],
]
OnJobPostrunType = t.Callable[
    [JobCtx, arq.worker.Function, ArgsType, KwargsType, t.Any],
    t.Awaitable[None],
]
OnJobPrepublishType = t.Callable[
    [DataDict, arq.worker.Function, ArgsType, KwargsType],
    t.Awaitable[None],
]
