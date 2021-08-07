import datetime as dt
import pathlib
from functools import partial
from typing import TYPE_CHECKING, Optional

import jinja2
import orjson

from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.loaders.csv.policy import (
    CSVLoadPolicy,
    CSVTransformPolicy,
)
from flowmaster.utils.logging_helper import Logger, getLogger

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook
    from flowmaster.operators.etl.dataschema import TransformContext, ETLContext


class CSVLoader:
    name = "csv"
    policy_model = CSVLoadPolicy
    transform_policy_model = CSVTransformPolicy
    data_orient = DataOrient.values

    _enter = False
    _is_add_columns = None

    def __init__(self, notebook: "ETLNotebook", logger: Optional[Logger] = None):
        self.notebook = notebook
        self.path = notebook.load.path
        self.save_mode = notebook.load.save_mode
        self.encoding = notebook.load.encoding
        self.sep = notebook.load.sep
        self.newline = notebook.load.newline
        self.with_columns = notebook.load.with_columns

        _template = dict(
            name=notebook.name,
            provider=notebook.provider,
            storage=notebook.storage,
            datetime=dt.datetime.now(),
        )
        self.file_name = jinja2.Template(notebook.load.file_name).render(**_template)
        self.add_data_before = jinja2.Template(notebook.load.add_data_before).render(
            **_template
        )
        self.add_data_after = jinja2.Template(notebook.load.add_data_after).render(
            **_template
        )

        self.logger = logger or getLogger()

        self.columns = None
        self.insert_counter = 0
        self.file_path = pathlib.Path(self.path) / self.file_name
        self.open_file = partial(open, self.file_path, encoding=self.encoding)

    def update_context(self, model: "ETLContext") -> None:
        model.path = str(self.file_path)

    def values_to_text(self, data: list) -> str:
        func_dump_value = lambda value: orjson.dumps(value).decode()
        func_row_to_line = lambda row: self.sep.join(map(func_dump_value, row))
        data = self.newline.join(map(func_row_to_line, data))
        data += self.newline

        return data

    def __enter__(self) -> "CSVLoader":
        self._enter = True
        self._is_add_columns = self.save_mode == "w" or not pathlib.Path.exists(
            self.file_path
        )
        if self.save_mode == "w" or not pathlib.Path.exists(self.file_path):
            with self.open_file(mode="w"):
                ...

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._enter = False
        self.insert_counter = 0

        with self.open_file(mode="a") as f:
            f.write(self.add_data_after)
        self.logger.info("Save data to file: {}", f.name)

    def __call__(self, context: "TransformContext", *args, **kwargs) -> None:
        if self._enter is False:
            raise Exception("Call through the context manager")

        self.columns = context.insert_columns

        text = self.values_to_text(context.data)

        # Add column names.
        if (
            self._is_add_columns
            and self.with_columns
            and self.columns
            and self.insert_counter == 0
        ):
            columns_str = self.sep.join(self.columns)
            text = f"{columns_str}\n{text}"

        if self.insert_counter == 0 and self.add_data_before:
            text = f"{self.add_data_before}\n{text}"

        with self.open_file(mode="a") as f:
            f.write(text)

        self.insert_counter += 1

        self.logger.info("Add data to file: {}", f.name)
