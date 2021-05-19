from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader
from flowmaster.operators.etl.loaders.csv.service import CSVLoader

storage_classes = {ClickhouseLoader.name: ClickhouseLoader, CSVLoader.name: CSVLoader}
