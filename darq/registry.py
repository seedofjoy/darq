import typing as t
from collections import UserDict

from . import worker

if t.TYPE_CHECKING:  # pragma: no cover
    BaseRegistry = UserDict[str, worker.Function]
else:
    BaseRegistry = UserDict


class Registry(BaseRegistry):

    def add(self, arq_function: 'worker.Function') -> None:
        self[arq_function.name] = arq_function

    def get_functions(self) -> t.Sequence['worker.Function']:
        return tuple(self.values())

    def get_function_names(self) -> t.Sequence[str]:
        return tuple(arq_func.name for arq_func in self.values())
