import typing as t
from collections import UserDict

import arq

if t.TYPE_CHECKING:
    BaseRegistry = UserDict[str, arq.worker.Function]
else:
    BaseRegistry = UserDict


class Registry(BaseRegistry):

    def add(self, arq_function: arq.worker.Function) -> None:
        self[arq_function.name] = arq_function

    def get_functions(self) -> t.Sequence[arq.worker.Function]:
        return tuple(self.values())

    def get_function_names(self) -> t.Sequence[str]:
        return tuple(arq_func.name for arq_func in self.values())
