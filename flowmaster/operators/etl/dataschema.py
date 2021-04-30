import datetime as dt
from typing import Optional, Union

import pydantic


class ETLContext(pydantic.BaseModel):
    storage: str
    provider: str
    start_period: dt.datetime = None
    end_period: dt.datetime = None
    size: Optional[pydantic.PositiveInt] = 0
    number_rows: Optional[pydantic.PositiveInt] = 0
    number_error_lines: Optional[pydantic.PositiveInt] = 0
    export_kwargs: dict = pydantic.Field(default_factory=dict)


class ExportContext(pydantic.BaseModel):
    columns: Union[list, tuple, set]
    data: list
    export_kwargs: dict = pydantic.Field(default_factory=dict)


class TransformContext(pydantic.BaseModel):
    size: int
    insert_columns: Union[list, tuple, set]
    partitions: Union[list, tuple, set]
    data: list
    data_errors: list
