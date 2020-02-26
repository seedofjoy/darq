import inspect
import typing as t

from .types import AnyCallable
from .types import JobCtx

JOB_CTX_KEYS = set(['job_id', 'job_try', 'enqueue_time', 'score'])


def get_function_name(func: AnyCallable) -> str:
    module_str = func.__module__
    if module_str == '__main__':
        module = inspect.getmodule(func)
        if module and module.__spec__:
            module_str = module.__spec__.name

    return f'{module_str}.{func.__name__}'


def split_job_ctx_from_args(
        args: t.Sequence[t.Any],
) -> t.Tuple[t.Optional[JobCtx], t.List[t.Any]]:
    ctx, args = None, list(args)
    if args:
        if isinstance(args[0], dict) and JOB_CTX_KEYS.intersection(args[0]):
            ctx = args.pop(0)
    return ctx, args
