from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.yandex_metrika_logs.export import (
    YandexMetrikaLogsExport,
)
from flowmaster.operators.etl.providers.yandex_metrika_logs.policy import (
    YandexMetrikaLogsExportPolicy,
)
from flowmaster.operators.etl.providers.yandex_metrika_logs.transform import (
    YandexMetrikaLogsTransform,
)


class YandexMetrikaLogsProvider(ProviderAbstract):
    name = "yandex_metrika_logs"
    export_class = YandexMetrikaLogsExport
    transform_class = YandexMetrikaLogsTransform
    policy_model = YandexMetrikaLogsExportPolicy
