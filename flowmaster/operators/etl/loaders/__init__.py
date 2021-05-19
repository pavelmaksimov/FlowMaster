from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoad
from flowmaster.operators.etl.loaders.csv.service import CSVLoader

storage_classes = {ClickhouseLoad.name: ClickhouseLoad, CSVLoader.name: CSVLoader}
