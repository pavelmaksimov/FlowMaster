from typing import Type, Optional

from loguru._logger import Logger

from flowmaster.operators.base.policy import BaseNotebook
from flowmaster.utils import KlassCollection
from .abstract import ProviderAbstract
from .criteo import CriteoProvider
from .csv import CSVProvider
from .fakedata import FakeDataProvider
from .flowmaster_data import FlowmasterDataProvider
from .google_sheets import GoogleSheetsProvider
from .mysql import MySQLProvider
from .postgres import PostgresProvider
from .sqlite import SQLiteProvider
from .yandex_direct import YandexDirectProvider
from .yandex_metrika_logs import YandexMetrikaLogsProvider
from .yandex_metrika_management import YandexMetrikaManagementProvider
from .yandex_metrika_stats import YandexMetrikaStatsProvider


class ProviderCollection(KlassCollection):
    def __getitem__(self, provider_name: str) -> Type[ProviderAbstract]: ...
    def get(self, provider_name: str) -> Type[ProviderAbstract]: ...
    def init(
        self,
        provider_name: str,
        notebook: BaseNotebook,
        logger: Optional[Logger] = None,
    ) -> ProviderAbstract: ...
    def load_provider_plugins(self) -> None: ...
    CSVProvider: Type[CSVProvider]
    CriteoProvider: Type[CriteoProvider]
    FakeDataProvider: Type[FakeDataProvider]
    FlowmasterDataProvider: Type[FlowmasterDataProvider]
    GoogleSheetsProvider: Type[GoogleSheetsProvider]
    PostgresProvider: Type[PostgresProvider]
    MySQLProvider: Type[MySQLProvider]
    SQLiteProvider: Type[SQLiteProvider]
    YandexMetrikaStatsProvider: Type[YandexMetrikaStatsProvider]
    YandexMetrikaManagementProvider: Type[YandexMetrikaManagementProvider]
    YandexMetrikaLogsProvider: Type[YandexMetrikaLogsProvider]
    YandexDirectProvider: Type[YandexDirectProvider]

Providers: ProviderCollection
