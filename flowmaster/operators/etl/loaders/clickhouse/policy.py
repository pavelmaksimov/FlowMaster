from typing import Union, List, Literal, Optional

from pydantic import BaseModel, Field

from flowmaster.operators.base.policy import BasePolicy


class ClickhouseLoadPolicy(BasePolicy):
    class Credentials(BaseModel):
        user: str
        host: str
        port: int = 9000
        password: Optional[Union[str, int]] = None

    class TableSchema(BaseModel):
        db: str
        table: str
        columns: List[str]
        orders: List[str]
        partition: Optional[Union[List[str], str]] = None

    credentials: Credentials
    table_schema: TableSchema
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
