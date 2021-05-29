from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.flowmaster_data.export import (
    FlowmasterDataExport,
)
from flowmaster.operators.etl.providers.flowmaster_data.policy import (
    FlowmasterDataExportPolicy,
)


class FlowmasterDataProvider(ProviderAbstract):
    name = "flowmaster"
    policy_model = FlowmasterDataExportPolicy
    export_class = FlowmasterDataExport
