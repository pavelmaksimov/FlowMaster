from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.mysql.export import MySQLExport
from flowmaster.operators.etl.providers.mysql.policy import MySQLExportPolicy


class MySQLProvider(ProviderAbstract):
    name = "mysql"
    policy_model = MySQLExportPolicy
    export_class = MySQLExport
