from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.csv.export import CSVExport
from flowmaster.operators.etl.providers.csv.policy import CSVExportPolicy


class CSVProvider(ProviderAbstract):
    name = "csv"
    policy_model = CSVExportPolicy
    export_class = CSVExport
