from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.sqlite.export import SQLiteExport
from flowmaster.operators.etl.providers.sqlite.policy import SQLiteExportPolicy


class SQLiteProvider(ProviderAbstract):
    name = "sqlite"
    policy_model = SQLiteExportPolicy
    export_class = SQLiteExport
