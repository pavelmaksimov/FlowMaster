from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoad
from flowmaster.operators.etl.loaders.file.service import FileLoad

storage_classes = {ClickhouseLoad.name: ClickhouseLoad, FileLoad.name: FileLoad}
