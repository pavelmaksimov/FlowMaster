from typing import Type, Union

from flowmaster.operators.etl.core import ETLOperator
from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.utils import KlassCollection

class KlassCollection(KlassCollection):
    ETLOperator: Type[ETLOperator]
    def __getitem__(self, operator_name: str) -> Type[ETLOperator]: ...
    def get(self, operator_name: str, /) -> Type[ETLOperator]: ...
    def __call__(self, notebook: Union[dict, ETLNotebook]) -> ETLOperator: ...
    def init(self, operator_name: str, /, notebook: ETLNotebook) -> ETLOperator: ...

Flow: KlassCollection
