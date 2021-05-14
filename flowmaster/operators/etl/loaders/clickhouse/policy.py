from typing import Union, List, Literal, Optional

from pydantic import BaseModel, Field

from flowmaster.operators.base.policy import BasePolicy
from flowmaster.operators.etl.transform.policy import (
    BaseTransformPolicy,
    ErrorPolicyLiteralT,
)


class ClickhouseLoadPolicy(BasePolicy):
    class CredentialsPolicy(BaseModel):
        user: str
        host: str
        port: int = 9000
        password: Optional[Union[str, int]] = None

    class TableSchemaPolicy(BaseModel):
        db: str
        table: str
        columns: List[str]
        orders: List[str]
        partition: Optional[Union[List[str], str]] = None

    credentials: CredentialsPolicy
    table_schema: TableSchemaPolicy
    data_cleaning_mode: Literal["partition", "off", "truncate"]
    sql_after: Optional[list[str]] = Field(default_factory=list)
    sql_before: Optional[list[str]] = Field(default_factory=list)
    concurrency: int = 2

    def __init__(self, **kwargs):
        table_schema = kwargs.get("table_schema", {})
        if isinstance(table_schema, dict):
            columns = table_schema.get("columns", None)
            if isinstance(columns, dict):
                table_schema["columns"] = list(columns.values())

        super(ClickhouseLoadPolicy, self).__init__(**kwargs)


class ClickhouseTransformPolicy(BaseTransformPolicy):
    class ColumnSchemaPolicy(BaseModel):
        errors: Optional[ErrorPolicyLiteralT] = None
        dt_format: Optional[str] = None
        null_values: Optional[list] = None
        clear_values: Optional[list] = None

    # {ExportColumnName: InsertColumnName}
    column_map: dict[str, str]
    column_schema: dict[str, ColumnSchemaPolicy] = Field(default_factory=dict)
