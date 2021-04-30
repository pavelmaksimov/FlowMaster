from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union

import pydantic

from flowmaster.operators.etl.loaders import FileLoad, ClickhouseLoad
from flowmaster.operators.etl.transform.policy import DTypeLiteralT
from flowmaster.operators.etl.transform.policy import ErrorPolicyLiteralT

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig


class TransformSchemaData(pydantic.BaseModel):
    name: str
    errors: ErrorPolicyLiteralT
    allow_null: bool
    null_value: Any
    null_values: Union[list, tuple, set]
    timezone: Optional[str] = None
    dtype: Optional[DTypeLiteralT] = None


class TransformSchemaDataList(pydantic.BaseModel):
    list: list[TransformSchemaData]


class StorageTransformSchemaAbstract(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def null_default_value(self) -> Any:
        pass

    @abstractmethod
    def create_column_schema(
        self, export_col_names: Union[list, tuple, set]
    ) -> list[dict]:
        pass


class FileTransformSchema(StorageTransformSchemaAbstract):
    name = FileLoad.name
    allow_null = False
    null_default_value = ""

    def __init__(self, config: "ETLFlowConfig", null_values: Union[list, tuple, set]):
        self.column_schema = {
            # TODO: refactoring .dict(exclude_unset=True)
            col_name: {k: v for k, v in col_schema.dict().items() if v is not None}
            for col_name, col_schema in config.transform.column_schema.items()
        }
        self.error_policy = config.transform.error_policy
        self.timezone = config.transform.timezone

        self.null_values = null_values

    def create_column_schema(
        self, export_columns: Union[list, tuple, set]
    ) -> TransformSchemaDataList:
        column_schema = []
        for export_colname in export_columns:
            column_schema.append(
                TransformSchemaData(
                    **{
                        "name": export_colname,
                        "errors": self.error_policy,
                        "timezone": self.timezone,
                        "allow_null": self.allow_null,
                        "null_value": self.null_default_value,
                        "null_values": self.null_values,
                        **self.column_schema.get(export_colname, {}),
                    }
                )
            )

        return TransformSchemaDataList(list=column_schema)


class ClickhouseTransformSchema(StorageTransformSchemaAbstract):
    name = ClickhouseLoad.name
    null_default_value = None

    def __init__(self, config: "ETLFlowConfig", null_values: Union[list, tuple, set]):
        self.column_schema = {
            col_name: {k: v for k, v in col_schema.dict().items() if v is not None}
            for col_name, col_schema in config.transform.column_schema.items()
        }
        self.column_matching = config.transform.column_matching
        self.table_schema_columns = config.load.table_schema.columns
        self.error_policy = config.transform.error_policy
        self.timezone = config.transform.timezone

        self.null_values = null_values

    def get_insert_col_name(self, export_colname: str) -> str:
        return self.column_matching[export_colname]

    def get_data_type(self, export_colname: str) -> DTypeLiteralT:
        dtype_map = {
            "String": "string",
            "UInt": "uint",
            "Int": "int",
            "Float": "float",
            "Decimal": "float",
            "DateTime": "datetime",
            "Date": "date",
        }
        column_name = self.get_insert_col_name(export_colname)
        for col_schema in self.table_schema_columns:
            col_schema = col_schema.split(" ")
            name, dtype = col_schema[0], col_schema[1]
            if name == column_name:
                for db_dtype in dtype_map:
                    if db_dtype in dtype:
                        return dtype_map[db_dtype]

        raise KeyError("Column type not matched")

    def get_null_policy(self, export_colname: str) -> bool:
        column_name = self.get_insert_col_name(export_colname)
        for col_schema in self.table_schema_columns:
            col_schema = col_schema.split(" ")
            name, dtype = col_schema[0], col_schema[1]
            if name == column_name:
                if "Nullable" in dtype:
                    return True

        return False

    def create_column_schema(
        self, export_col_names: Union[list, tuple, set]
    ) -> TransformSchemaDataList:
        column_schema = []
        for export_colname in export_col_names:
            column_schema.append(
                TransformSchemaData(
                    **{
                        "name": self.get_insert_col_name(export_colname),
                        "dtype": self.get_data_type(export_colname),
                        "errors": self.error_policy,
                        "timezone": self.timezone,
                        "allow_null": self.get_null_policy(export_colname),
                        "null_value": self.null_default_value,
                        "null_values": self.null_values,
                        **self.column_schema.get(export_colname, {}),
                    }
                )
            )

        return TransformSchemaDataList(list=column_schema)
