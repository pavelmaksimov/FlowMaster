from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.postgres.export import PostgresExport
from flowmaster.operators.etl.providers.postgres.policy import PostgresExportPolicy


class PostgresProvider(ProviderAbstract):
    name = "postgres"
    policy_model = PostgresExportPolicy
    export_class = PostgresExport
