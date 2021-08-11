from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader
from flowmaster.operators.etl.loaders.csv.service import CSVLoader
from flowmaster.utils import KlassCollection


class StoragesCollection(KlassCollection):
    name_attr_of_klass = "storage"


Storages = StoragesCollection(ClickhouseLoader, CSVLoader)
