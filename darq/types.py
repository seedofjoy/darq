import datetime
import sys
import typing as t

if sys.version_info >= (3, 8):
    from typing import Protocol
    from typing import TypedDict
else:
    from typing_extensions import Protocol
    from typing_extensions import TypedDict

if t.TYPE_CHECKING:  # pragma: no cover
    import darq
    from darq.jobs import Job  # noqa F401
    from darq.worker import Task  # noqa F401

UNSET_ARG = t.NewType('UNSET_ARG', object)
unset_arg: UNSET_ARG = UNSET_ARG(object())

WrappingFunc = t.TypeVar(
    'WrappingFunc', bound=t.Callable[..., t.Coroutine[t.Any, t.Any, t.Any]],
)
T = t.TypeVar('T')


class DarqTask(Protocol[T]):
    __call__: T
    delay: T
    __name__: str
    __qualname__: str

    @staticmethod
    async def apply_async(
            args: t.Optional[t.Sequence[t.Any]] = None,
            kwargs: t.Optional[t.Mapping[str, t.Any]] = None,
            *,
            job_id: t.Optional[str] = None,
            queue: t.Optional[str] = None,
            defer_until: t.Optional[datetime.datetime] = None,
            defer_by: t.Optional['AnyTimedelta'] = None,
            expires: t.Union[None, 'AnyTimedelta', UNSET_ARG] = unset_arg,
            job_try: t.Optional[int] = None,
    ) -> t.Optional['Job']:  # pragma: no cover
        ...


AnyCallable = t.Callable[..., t.Any]
CoroutineType = t.Callable[..., t.Coroutine[t.Any, t.Any, t.Any]]
AnyTimedelta = t.Union[int, float, datetime.timedelta]
AnyDict = t.Dict[t.Any, t.Any]
DataDict = t.Dict[str, t.Any]
ArgsType = t.Sequence[t.Any]
KwargsType = t.Mapping[str, t.Any]
MutableArgsType = t.List[t.Any]
MutableKwargsType = t.Dict[str, t.Any]


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
    [AnyDict, 'Task', ArgsType, KwargsType],
    t.Awaitable[None],
]
OnJobPostrunType = t.Callable[
    [AnyDict, 'Task', ArgsType, KwargsType, t.Any],
    t.Awaitable[None],
]
OnJobPrepublishType = t.Callable[
    [
        DataDict, 'Task', MutableArgsType, MutableKwargsType,
        JobEnqueueOptions,
    ],
    t.Awaitable[None],
]
