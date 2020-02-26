import inspect

from .types import AnyCallable


def get_function_name(func: AnyCallable) -> str:
    module_str = func.__module__
    if module_str == '__main__':
        module = inspect.getmodule(func)
        if module and module.__spec__:
            module_str = module.__spec__.name

    return f'{module_str}.{func.__name__}'
