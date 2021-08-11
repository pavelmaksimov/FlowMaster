import importlib

from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.criteo import CriteoProvider
from flowmaster.operators.etl.providers.csv import CSVProvider
from flowmaster.operators.etl.providers.fakedata import FakeDataProvider
from flowmaster.operators.etl.providers.flowmaster_data import FlowmasterDataProvider
from flowmaster.operators.etl.providers.google_sheets import GoogleSheetsProvider
from flowmaster.operators.etl.providers.mysql import MySQLProvider
from flowmaster.operators.etl.providers.postgres import PostgresProvider
from flowmaster.operators.etl.providers.sqlite import SQLiteProvider
from flowmaster.operators.etl.providers.yandex_direct import YandexDirectProvider
from flowmaster.operators.etl.providers.yandex_metrika_logs import (
    YandexMetrikaLogsProvider,
)
from flowmaster.operators.etl.providers.yandex_metrika_management import (
    YandexMetrikaManagementProvider,
)
from flowmaster.operators.etl.providers.yandex_metrika_stats import (
    YandexMetrikaStatsProvider,
)
from flowmaster.setttings import Settings
from flowmaster.utils import KlassCollection
from flowmaster.utils.import_helper import iter_module_objects


class ProviderCollection(KlassCollection):
    name_attr_of_klass = "provider"

    def __init__(self, *args):
        super(ProviderCollection, self).__init__(*args)
        self.load_provider_plugins()

    def load_provider_plugins(self) -> None:
        """Fetching custom providers."""
        for object in iter_module_objects(
            importlib.import_module(Settings.PLUGINS_DIRNAME)
        ):
            if (
                isinstance(object, type)
                and object.__name__ != ProviderAbstract.__name__
                and issubclass(object, ProviderAbstract)
            ):
                self.set(object)


Providers = ProviderCollection(
    CSVProvider,
    CriteoProvider,
    FakeDataProvider,
    FlowmasterDataProvider,
    GoogleSheetsProvider,
    PostgresProvider,
    MySQLProvider,
    SQLiteProvider,
    YandexMetrikaStatsProvider,
    YandexMetrikaManagementProvider,
    YandexMetrikaLogsProvider,
    YandexDirectProvider,
)
