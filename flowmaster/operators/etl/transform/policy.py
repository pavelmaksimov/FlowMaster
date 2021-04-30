from typing import Literal, Optional, TypeVar

from pydantic import BaseModel

from flowmaster.operators.base.policy import BasePolicy

ErrorPolicyLiteralT = TypeVar(
    "ErrorPolicyLiteralT", bound=Literal["raise", "default", "coerce", "ignore"]
)
DTypeLiteralT = TypeVar(
    "DTypeLiteralT",
    bound=Literal[
        "array", "timestamp", "string", "uint", "int", "float", "datetime", "date"
    ],
)


class BaseTransformPolicy(BasePolicy):
    error_policy: ErrorPolicyLiteralT
    timezone: Optional[str] = None
    partition_columns: Optional[list] = None
    column_schema: dict[str, BaseModel] = {}
    concurrency: int = 100


class FileTransformPolicy(BaseTransformPolicy):
    class ColumnSchema(BaseModel):
        name: Optional[str] = None
        dtype: Optional[DTypeLiteralT] = None
        errors: Optional[ErrorPolicyLiteralT] = None
        dt_format: Optional[str] = None
        allow_null: Optional[bool] = None
        null_values: Optional[list] = None
        clear_values: Optional[list] = None

    column_schema: dict[str, ColumnSchema] = {}


class ClickhouseTransformPolicy(BaseTransformPolicy):
    class ColumnSchema(BaseModel):
        errors: Optional[ErrorPolicyLiteralT] = None
        dt_format: Optional[str] = None
        null_values: Optional[list] = None
        clear_values: Optional[list] = None

    column_matching: dict[str, str]
    column_schema: dict[str, ColumnSchema] = {}
