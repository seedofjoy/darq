import inspect

from .types import AnyCallable


def get_function_name(func: AnyCallable) -> str:
    module = func.__module__
    if module == '__main__':
        module = inspect.getmodule(func).__spec__.name  # type: ignore

    return f'{module}.{func.__name__}'
