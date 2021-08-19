from typing import Type, Union, Optional

from loguru._logger import Logger

from flowmaster.operators.base.policy import BaseNotebook
from flowmaster.utils import KlassCollection
from .clickhouse.service import ClickhouseLoader
from .csv.service import CSVLoader


class KlassCollection(KlassCollection):
    def __getitem__(self, storage_name: str) -> Type[ClickhouseLoader, CSVLoader]: ...
    def get(self, storage_name: str) -> Type[ClickhouseLoader, CSVLoader]: ...
    def init(
        self,
        storage_name: str,
        notebook: BaseNotebook,
        logger: Optional[Logger] = None,
    ) -> Union[ClickhouseLoader, CSVLoader]: ...
    ClickhouseLoader: Type[ClickhouseLoader]
    CSVLoader: Type[CSVLoader]

Storages: KlassCollection
