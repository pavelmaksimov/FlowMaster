import datetime as dt
from typing import Optional, Union

import pydantic

from flowmaster.operators.etl.enums import DataOrient, ETLSteps


class OperatorContext(pydantic.BaseModel):
    operator: str


class ETLContext(OperatorContext):
    storage: str
    db: str = None
    table: str = None
    path: str = None
    step: Optional[ETLSteps.LiteralT] = None
    start_period: dt.datetime = None
    end_period: dt.datetime = None
    size: Optional[pydantic.PositiveInt] = 0
    number_rows: Optional[pydantic.PositiveInt] = 0
    number_error_lines: Optional[pydantic.PositiveInt] = 0
    export_kwargs: dict = pydantic.Field(default_factory=dict)


class ExportContext(pydantic.BaseModel):
    columns: Union[list, tuple, set]
    data: list
    data_orient: DataOrient.LiteralT
    request_kwargs: dict = pydantic.Field(default_factory=dict)
    response_kwargs: dict = pydantic.Field(default_factory=dict)


class TransformContext(pydantic.BaseModel):
    size: int
    insert_columns: Union[list, tuple, set]
    partitions: Union[list, tuple, set]
    data: list
    data_errors: list
