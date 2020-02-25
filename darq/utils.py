import inspect
import typing as t

from .types import AnyCallable

JOB_CTX_KEYS = set(['job_id', 'job_try', 'enqueue_time', 'score'])


def get_function_name(func: AnyCallable) -> str:
    module_str = func.__module__
    if module_str == '__main__':
        module = inspect.getmodule(func)
        if module and module.__spec__:
            module_str = module.__spec__.name

    return f'{module_str}.{func.__name__}'


def is_signature_has_param(func: AnyCallable, param: str) -> bool:
    return param in inspect.signature(func).parameters


def split_job_ctx_from_args(args: t.Sequence[t.Any]) -> t.Tuple:
    ctx, args = None, list(args)
    if args:
        if isinstance(args[0], dict) and JOB_CTX_KEYS.intersection(args[0]):
            ctx = args.pop(0)
    return ctx, args
