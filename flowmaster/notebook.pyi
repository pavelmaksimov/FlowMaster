from typing import Type

from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.utils import KlassCollection


class NotebooksCollection(KlassCollection):
    ETLNotebook: Type[ETLNotebook]
    def __getitem__(self, operator_name: str) -> Type[ETLNotebook]: ...
    def get(self, operator_name: str, /) -> Type[ETLNotebook]: ...
    def __call__(self, **notebook_kwargs) -> ETLNotebook: ...
    def init(self, operator_name: str, /, **notebook_kwargs) -> ETLNotebook: ...

Notebooks: NotebooksCollection
