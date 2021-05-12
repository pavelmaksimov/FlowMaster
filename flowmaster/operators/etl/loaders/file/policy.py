from typing import Literal, Optional

from pydantic import BaseModel, Field

from flowmaster.operators.base.policy import BasePolicy
from flowmaster.operators.etl.transform.policy import BaseTransformPolicy, DTypeLiteralT, ErrorPolicyLiteralT
from flowmaster.setttings import FILE_STORAGE_DIR


class FileLoadPolicy(BasePolicy):
    save_mode: Literal["a", "w"]
    file_name: str = "{{provider}} {{storage}}  {{name}}.tsv"
    path: str = FILE_STORAGE_DIR
    encoding: str = "UTF-8"
    sep: str = "\t"
    newline: str = "\n"
    with_columns: bool = True
    add_data_before: str = ""
    add_data_after: str = ""
    concurrency: int = 1


class FileTransformPolicy(BaseTransformPolicy):
    class ColumnSchema(BaseModel):
        name: Optional[str] = None
        dtype: Optional[DTypeLiteralT] = None
        errors: Optional[ErrorPolicyLiteralT] = None
        dt_format: Optional[str] = None
        allow_null: Optional[bool] = None
        null_values: Optional[list] = None
        clear_values: Optional[list] = None

    column_schema: dict[str, ColumnSchema] = Field(default_factory=dict)
