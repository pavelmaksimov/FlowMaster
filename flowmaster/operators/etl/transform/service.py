from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any, Optional, Union

from datagun import DataSet, NULL_VALUES

from flowmaster.operators.etl.dataschema import TransformContext
from flowmaster.operators.etl.transform.tschema import (
    FileTransformSchema,
    ClickhouseTransformSchema,
)

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig
    from flowmaster.operators.etl.transform import TransformSchemaDataList
    from flowmaster.operators.etl.types import DataOrient

    DataOrientLiteralT = DataOrient.LiteralT


class Transform:
    null_values = NULL_VALUES

    _schema_classes = {
        FileTransformSchema.name: FileTransformSchema,
        ClickhouseTransformSchema.name: ClickhouseTransformSchema,
    }

    def __init__(self, config: "ETLFlowConfig", logger: Optional[Logger] = None):
        self.config = config
        self.storage = config.storage
        self.error_policy = config.transform.error_policy
        self.partition_columns = config.transform.partition_columns

        self.Schema = self._schema_classes[self.storage](
            config=config, null_values=self.null_values
        )
        self.logger = logger or getLogger("Transform")

    def processing(
        self,
        data: Any,
        column_schema: "TransformSchemaDataList",
        orient: "DataOrientLiteralT",
    ) -> DataSet:
        return DataSet(data, schema=column_schema.dict()["list"], orient=orient)

    def __call__(
        self,
        data: Any,
        export_columns: Union[list, tuple, set],
        export_data_orient: "DataOrientLiteralT",
        load_data_orient: "DataOrientLiteralT",
    ) -> TransformContext:
        from flowmaster.operators.etl import DataOrient

        column_schema = self.Schema.create_column_schema(export_columns)
        assert column_schema is not None

        dataset = self.processing(
            data, column_schema=column_schema, orient=export_data_orient
        )

        assert isinstance(dataset, DataSet)

        if load_data_orient == DataOrient.values:
            data = dataset.to_values()
        elif load_data_orient == DataOrient.columns:
            data = dataset.to_list()
        elif load_data_orient == DataOrient.dict:
            data = dataset.to_dict()
        else:
            raise NotImplementedError(f"{load_data_orient=} not supported")

        if self.partition_columns:
            partitions = dataset[self.partition_columns].distinct().to_values()
        else:
            partitions = []

        size, insert_columns, partitions, data, data_errors = (
            dataset.size,
            dataset.columns,
            partitions,
            data,
            dataset.get_errors().to_values(),
        )

        return TransformContext(
            size=size,
            insert_columns=insert_columns,
            partitions=partitions,
            data=data,
            data_errors=data_errors,
        )
