from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.yandex_metrika_management.export import (
    YandexMetrikaManagementExport,
)
from flowmaster.operators.etl.providers.yandex_metrika_management.policy import (
    YandexMetrikaManagementExportPolicy,
)
from flowmaster.operators.etl.providers.yandex_metrika_management.transform import (
    YandexMetrikaManagementTransform,
)


class YandexMetrikaManagementProvider(ProviderAbstract):
    name = "yandex_metrika_management"
    export_class = YandexMetrikaManagementExport
    transform_class = YandexMetrikaManagementTransform
    policy_model = YandexMetrikaManagementExportPolicy
