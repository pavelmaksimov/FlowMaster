from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.yandex_metrika_stats.export import (
    YandexMetrikaStatsExport,
)
from flowmaster.operators.etl.providers.yandex_metrika_stats.policy import (
    YandexMetrikaStatsExportPolicy,
)
from flowmaster.operators.etl.providers.yandex_metrika_stats.transform import (
    YandexMetrikaStatsTransform,
)


class YandexMetrikaStatsProvider(ProviderAbstract):
    name = "yandex_metrika_stats"
    export_class = YandexMetrikaStatsExport
    transform_class = YandexMetrikaStatsTransform
    policy_model = YandexMetrikaStatsExportPolicy
