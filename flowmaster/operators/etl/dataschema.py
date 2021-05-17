import datetime as dt
from typing import Optional, Union

import pydantic

from flowmaster.operators.etl.types import DataOrient


class ETLContext(pydantic.BaseModel):
    storage: str
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
    export_kwargs: dict = pydantic.Field(default_factory=dict)


class TransformContext(pydantic.BaseModel):
    size: int
    insert_columns: Union[list, tuple, set]
    partitions: Union[list, tuple, set]
    data: list
    data_errors: list
