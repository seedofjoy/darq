import typing as t
from collections import UserDict

import arq

from .types import AnyCallable

if t.TYPE_CHECKING:
    BaseRegistry = UserDict[str, arq.worker.Function]
else:
    BaseRegistry = UserDict


class Registry(BaseRegistry):

    def __init__(self) -> None:
        super().__init__()
        self.by_original_coro: t.Dict[AnyCallable, arq.worker.Function] = {}

    def __setitem__(
            self, arq_function_name: str, arq_function: arq.worker.Function,
    ) -> None:
        original_coroutine = t.cast(
            AnyCallable,
            arq_function.coroutine.__wrapped__,  # type: ignore
        )
        self.by_original_coro[original_coroutine] = arq_function
        self.data[arq_function_name] = arq_function

    def __delitem__(self, arq_function_name: str) -> None:
        arq_function = self.data[arq_function_name]
        original_coroutine = t.cast(
            AnyCallable,
            arq_function.coroutine.__wrapped__,  # type: ignore
        )
        del self.by_original_coro[original_coroutine]
        del self.data[arq_function_name]

    def add(self, arq_function: arq.worker.Function) -> None:
        self[arq_function.name] = arq_function

    def get_functions(self) -> t.Sequence[arq.worker.Function]:
        return tuple(self.values())

    def get_function_names(self) -> t.Sequence[str]:
        return tuple(arq_func.name for arq_func in self.values())
