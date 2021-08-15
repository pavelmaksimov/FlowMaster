from typing import TYPE_CHECKING, Any, Optional

from datagun import DataSet, NULL_VALUES

from flowmaster.operators.etl.dataschema import TransformContext
from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.transform.tschema import (
    FileTransformSchema,
    ClickhouseTransformSchema,
)
from flowmaster.utils.logging_helper import Logger, getLogger

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.dataschema import ExportContext


class Transform:
    null_values = NULL_VALUES

    _schema_classes = {
        FileTransformSchema.name: FileTransformSchema,
        ClickhouseTransformSchema.name: ClickhouseTransformSchema,
    }

    def __init__(self, notebook: "ETLNotebookPolicy", logger: Optional[Logger] = None):
        self.notebook = notebook
        self.storage = notebook.storage
        self.error_policy = notebook.transform.error_policy
        self.partition_columns = notebook.transform.partition_columns

        self.Schema = self._schema_classes[self.storage](
            notebook=notebook, null_values=self.null_values
        )
        self.logger = logger or getLogger()

    def processing(
        self,
        data: Any,
        column_schema: list[dict],
        orient: DataOrient.LiteralT,
    ) -> DataSet:
        return DataSet(data, schema=column_schema, orient=orient)

    def changing_data_orient_for_storage(self, dataset, storage_data_orient):
        if storage_data_orient == DataOrient.values:
            data = dataset.to_values()
        elif storage_data_orient == DataOrient.columns:
            data = dataset.to_list()
        elif storage_data_orient == DataOrient.dict:
            data = dataset.to_dict()
        else:
            raise NotImplementedError(f"{storage_data_orient=} not supported")

        return data

    def __call__(
        self, export_context: "ExportContext", storage_data_orient: DataOrient.LiteralT
    ) -> TransformContext:
        column_schema = self.Schema.create_column_schema(export_context.columns)
        assert column_schema is not None

        dataset = self.processing(
            export_context.data,
            column_schema=column_schema.dict()["list"],
            orient=export_context.data_orient,
        )
        assert isinstance(dataset, DataSet)

        dataset.rename_columns({sch.name: sch.new_name for sch in column_schema.list})

        data = self.changing_data_orient_for_storage(dataset, storage_data_orient)

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
