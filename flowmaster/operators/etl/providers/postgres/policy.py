from typing import Optional

from pydantic import PositiveInt

from flowmaster.operators.base.policy import BasePolicy, validator


class PostgresExportPolicy(BasePolicy):
    user: str
    password: str
    host: str
    port: PositiveInt = 5432
    database: str
    table: str
    columns: list[str]
    where: str = ""
    order_by: str = ""
    chunk_size: PositiveInt = 10000
    sql: Optional[str] = None
    sql_before: Optional[list[str]] = None
    sql_after: Optional[list[str]] = None

    @validator("sql")
    def validator_sql(cls, sql: str, **kwargs) -> str:
        if sql is not None:
            if any((kwargs["where"] == "", kwargs["order_by"] == "")):
                raise ValueError(
                    "When the 'sql' field is full, the 'where' and 'order_by' must be empty"
                )

        return sql

    @validator("where")
    def validator_where(cls, where: str, **kwargs) -> str:
        if where and "WHERE" not in where:
            where = "\nWHERE " + where

        return where

    @validator("order_by")
    def validator_order_by(cls, order_by: str, **kwargs) -> str:
        if order_by and "ORDER BY" not in order_by:
            order_by = "\nORDER BY " + order_by

        return order_by
