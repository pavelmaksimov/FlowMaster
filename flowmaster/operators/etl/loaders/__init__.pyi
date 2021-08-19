from typing import Type, Union, Optional

from loguru._logger import Logger

from flowmaster.operators.base.policy import PydanticModelT
from flowmaster.utils import KlassCollection
from .clickhouse.service import ClickhouseLoader
from .csv.service import CSVLoader


class LoadersCollection(KlassCollection):
    def __getitem__(self, storage_name: str) -> Type[ClickhouseLoader, CSVLoader]: ...
    def get(self, storage_name: str, /) -> Type[ClickhouseLoader, CSVLoader]: ...
    def __call__(
        self,
        notebook: PydanticModelT,
        logger: Optional[Logger] = None,
    ) -> Union[ClickhouseLoader, CSVLoader]: ...
    def init(
        self,
        storage_name: str,
        /,
        notebook: PydanticModelT,
        logger: Optional[Logger] = None,
    ) -> Union[ClickhouseLoader, CSVLoader]: ...
    ClickhouseLoader: Type[ClickhouseLoader]
    CSVLoader: Type[CSVLoader]

Storages: LoadersCollection
