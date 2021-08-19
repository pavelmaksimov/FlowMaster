from typing import TYPE_CHECKING, Any, Optional

from datagun import DataSet, NULL_VALUES

from flowmaster.operators.etl.dataschema import TransformContext
from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.loaders import Storages
from flowmaster.operators.etl.transform import TransformSchemas
from flowmaster.utils.logging_helper import Logger, getLogger

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook
    from flowmaster.operators.etl.dataschema import ExportContext


class Transform:
    null_values = NULL_VALUES

    def __init__(self, notebook: "ETLNotebook", logger: Optional[Logger] = None):
        self.notebook = notebook
        self.error_policy = notebook.transform.error_policy
        self.partition_columns = notebook.transform.partition_columns

        self.Schema = TransformSchemas.init(
            notebook.storage, notebook, null_values=self.null_values
        )
        self.logger = logger or getLogger()

    def processing(
        self,
        data: Any,
        column_schema: list[dict],
        orient: DataOrient.LiteralT,
    ) -> DataSet:
        return DataSet(data, schema=column_schema, orient=orient)

    def changing_data_orient_for_storage(self, dataset):
        Storage = Storages[self.notebook.storage]

        if Storage.data_orient == DataOrient.values:
            data = dataset.to_values()
        elif Storage.data_orient == DataOrient.columns:
            data = dataset.to_list()
        elif Storage.data_orient == DataOrient.dict:
            data = dataset.to_dict()
        else:
            raise NotImplementedError(f"{Storage.data_orient=} not supported")

        return data

    def __call__(self, export_context: "ExportContext") -> TransformContext:
        column_schema_list = self.Schema.create_column_schema(export_context.columns)
        assert column_schema_list is not None
        column_schema_list_dict = [m.dict() for m in column_schema_list]

        dataset = self.processing(
            export_context.data,
            column_schema=column_schema_list_dict,
            orient=export_context.data_orient,
        )
        assert isinstance(dataset, DataSet)

        dataset.rename_columns({sch.name: sch.new_name for sch in column_schema_list})

        data = self.changing_data_orient_for_storage(dataset)

        if self.partition_columns:
            partitions = dataset[self.partition_columns].distinct().to_values()
        else:
            partitions = []

        return TransformContext(
            size=dataset.size,
            insert_columns=dataset.columns,
            partitions=partitions,
            data=data,
            data_errors=dataset.get_errors().to_values(),
        )
