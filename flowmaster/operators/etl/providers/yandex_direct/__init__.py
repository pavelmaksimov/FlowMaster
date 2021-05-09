from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.yandex_direct.export import (
    YandexDirectExport,
)
from flowmaster.operators.etl.providers.yandex_direct.policy import (
    YandexDirectExportPolicy,
)
from flowmaster.operators.etl.providers.yandex_direct.transform import (
    YandexDirectTransform,
)


class YandexDirectProvider(ProviderAbstract):
    name = "yandex_direct"
    export_class = YandexDirectExport
    transform_class = YandexDirectTransform
    policy_model = YandexDirectExportPolicy
