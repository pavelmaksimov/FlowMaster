import importlib

from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.fakedata import FakeDataProvider
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
from flowmaster.setttings import PLUGINS_DIRNAME
from flowmaster.utils.import_helper import iter_module_objects

provider_classes = {
    FakeDataProvider.name: FakeDataProvider,
    YandexMetrikaStatsProvider.name: YandexMetrikaStatsProvider,
    YandexMetrikaManagementProvider.name: YandexMetrikaManagementProvider,
    YandexMetrikaLogsProvider.name: YandexMetrikaLogsProvider,
    YandexDirectProvider.name: YandexDirectProvider
}

# Fetching custom providers.
for object in iter_module_objects(importlib.import_module(PLUGINS_DIRNAME)):
    if (
        isinstance(object, type)
        and object.__name__ != ProviderAbstract.__name__
        and issubclass(object, ProviderAbstract)
    ):
        provider_classes[object.name] = object
