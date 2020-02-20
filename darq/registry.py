import typing as t
from collections import UserDict

import arq

from .utils import get_function_name

if t.TYPE_CHECKING:
    BaseRegistry = UserDict[str, arq.worker.Function]
else:
    BaseRegistry = UserDict


class Registry(BaseRegistry):

    def add(self, arq_function: arq.worker.Function) -> None:
        coro = arq_function.coroutine
        import_str = get_function_name(coro)
        self[import_str] = arq_function

    def get_function_names(self) -> t.Sequence[str]:
        return tuple(arq_func.name for arq_func in self.values())
