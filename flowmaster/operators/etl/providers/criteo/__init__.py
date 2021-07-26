from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.criteo.export import (
    CriteoExport,
)
from flowmaster.operators.etl.providers.criteo.policy import (
    CriteoExportPolicy,
)
from flowmaster.operators.etl.providers.criteo.transform import (
    CriteoTransform,
)


class CriteoProvider(ProviderAbstract):
    name = "criteo"
    export_class = CriteoExport
    transform_class = CriteoTransform
    policy_model = CriteoExportPolicy
