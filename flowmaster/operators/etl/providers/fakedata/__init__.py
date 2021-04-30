from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.fakedata.export import FakeDataExport
from flowmaster.operators.etl.providers.fakedata.policy import FakeDataExportPolicy


class FakeDataProvider(ProviderAbstract):
    name = "fakedata"
    policy_model = FakeDataExportPolicy
    export_class = FakeDataExport
