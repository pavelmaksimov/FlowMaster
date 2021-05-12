import datetime as dt
import pathlib
from functools import partial
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Optional

import jinja2
import orjson

from flowmaster.operators.etl.loaders.file.policy import FileLoadPolicy, FileTransformPolicy
from flowmaster.operators.etl.types import DataOrient

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig
    from flowmaster.operators.etl.dataschema import TransformContext


class FileLoad:
    name = "file"
    policy_model = FileLoadPolicy
    transform_policy_model = FileTransformPolicy
    data_orient = DataOrient.values

    _enter = False
    _is_add_columns = None

    def __init__(self, config: "ETLFlowConfig", logger: Optional[Logger] = None):
        self.config = config
        self.path = config.load.path
        self.save_mode = config.load.save_mode
        self.encoding = config.load.encoding
        self.sep = config.load.sep
        self.newline = config.load.newline
        self.with_columns = config.load.with_columns

        _template = dict(
            name=config.name,
            provider=config.provider,
            storage=config.storage,
            datetime=dt.datetime.now(),
        )
        self.file_name = jinja2.Template(config.load.file_name).render(**_template)
        self.add_data_before = jinja2.Template(config.load.add_data_before).render(
            **_template
        )
        self.add_data_after = jinja2.Template(config.load.add_data_after).render(
            **_template
        )

        self.logger = logger or getLogger("FileLoad")

        self.columns = None
        self.insert_counter = 0
        self.file_path = pathlib.Path(self.path) / self.file_name
        self.open_file = partial(open, self.file_path, encoding=self.encoding)

    def values_to_text(self, data):
        func_dump_value = lambda value: orjson.dumps(value).decode()
        func_row_to_line = lambda row: self.sep.join(map(func_dump_value, row))
        data = self.newline.join(map(func_row_to_line, data))
        data += self.newline

        return data

    def __enter__(self):
        self._enter = True
        self._is_add_columns = self.save_mode == "w" or not pathlib.Path.exists(
            self.file_path
        )
        if not pathlib.Path.exists(self.file_path):
            with self.open_file(mode="w"):
                ...

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._enter = False
        self.insert_counter = 0

        with self.open_file(mode="a") as f:
            f.write(self.add_data_after)

        self.logger.info(f"Save data to file: {f.name}")

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

        self.logger.info(f"Add data to file: {f.name}")
