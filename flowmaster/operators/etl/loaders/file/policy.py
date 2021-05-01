from typing import Literal

from flowmaster.operators.base.policy import BasePolicy
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
