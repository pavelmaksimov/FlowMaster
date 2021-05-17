from pydantic import PositiveInt

from flowmaster.operators.base.policy import BasePolicy


class CSVExportPolicy(BasePolicy):
    file_path: str
    with_columns: bool
    columns: list[str]
    sep: str = "\t"
    newline: str = "\n"
    encoding: str = "UTF-8"
    skip_begin_lines: int = 0
    chunk_size: PositiveInt = 10000
    concurrency: int = 1
