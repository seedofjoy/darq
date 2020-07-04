import typing as t
from collections import UserDict

from . import worker

if t.TYPE_CHECKING:  # pragma: no cover
    BaseRegistry = UserDict[str, worker.Task]
else:
    BaseRegistry = UserDict


class Registry(BaseRegistry):

    def add(self, task: 'worker.Task') -> None:
        self[task.name] = task

    def get_functions(self) -> t.Sequence['worker.Task']:
        return tuple(self.values())

    def get_function_names(self) -> t.Sequence[str]:
        return tuple(task.name for task in self.values())
