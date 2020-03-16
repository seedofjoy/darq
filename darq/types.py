import datetime
import sys
import typing as t

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

if t.TYPE_CHECKING:
    import darq
    from darq.cron import CronJob  # noqa F401

AnyCallable = t.Callable[..., t.Any]
AnyTimedelta = t.Union[int, float, datetime.timedelta]
AnyDict = t.Dict[t.Any, t.Any]
DataDict = t.Dict[str, t.Any]
ArgsType = t.Sequence[t.Any]
KwargsType = t.Mapping[str, t.Any]

FunctionOrCronJob = t.Union['darq.worker.Function', 'CronJob']


class JobCtx(TypedDict):
    redis: 'darq.connections.ArqRedis'
    job_id: str
    job_try: int
    enqueue_time: datetime.datetime
    score: int
    metadata: DataDict


class JobEnqueueOptions(TypedDict):
    job_id: t.Optional[str]
    queue_name: t.Optional[str]
    defer_until: t.Optional[datetime.datetime]
    defer_by: t.Optional[AnyTimedelta]
    expires: t.Optional[AnyTimedelta]
    job_try: t.Optional[int]


OnJobPrerunType = t.Callable[
    [AnyDict, FunctionOrCronJob, ArgsType, KwargsType],
    t.Awaitable[None],
]
OnJobPostrunType = t.Callable[
    [AnyDict, FunctionOrCronJob, ArgsType, KwargsType, t.Any],
    t.Awaitable[None],
]
OnJobPrepublishType = t.Callable[
    [DataDict, FunctionOrCronJob, ArgsType, KwargsType, JobEnqueueOptions],
    t.Awaitable[None],
]
