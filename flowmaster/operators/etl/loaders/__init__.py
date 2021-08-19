from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader
from flowmaster.operators.etl.loaders.csv.service import CSVLoader
from flowmaster.utils import KlassCollection


class LoadersCollection(KlassCollection):
    name_attr_in_kwargs = "storage"

Loaders = LoadersCollection(ClickhouseLoader, CSVLoader)
