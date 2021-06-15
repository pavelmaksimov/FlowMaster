import random
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Optional

import clickhousepy

from flowmaster.operators.etl.loaders.clickhouse.policy import (
    ClickhouseLoadPolicy,
    ClickhouseTransformPolicy,
)
from flowmaster.operators.etl.types import DataOrient

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook
    from flowmaster.operators.etl.dataschema import TransformContext, ETLContext


class ClickhouseLoader:
    class DataCleaningMode:
        """Clears the table before inserting data."""

        truncate = "truncate"
        off = "off"
        partition = "partition"

    name = "clickhouse"
    policy_model = ClickhouseLoadPolicy
    transform_policy_model = ClickhouseTransformPolicy

    StageTable: Optional[clickhousepy.Table] = None
    data_orient = DataOrient.columns

    def __init__(self, notebook: "ETLNotebook", logger: Optional[Logger] = None):
        self.data_cleaning_mode = notebook.load.data_cleaning_mode
        self.credentials = notebook.load.credentials.dict()
        self.create_table_config = notebook.load.table_schema.dict()
        self.sql_after = notebook.load.sql_after
        self.sql_before = notebook.load.sql_before

        self.DB = self.client.DB(self.create_table_config["db"])
        self.Table = self.client.Table(
            self.create_table_config["db"], self.create_table_config["table"]
        )
        self.partitions = []
        self.logger = logger or getLogger("ClickhouseLoader")

    def set_context(self, model: "ETLContext") -> None:
        model.db = self.Table.db
        model.table = self.Table.table

    @property
    def client(self) -> clickhousepy.Client:
        self.credentials["password"] = str(self.credentials["password"])
        return clickhousepy.Client(**self.credentials)

    def validate_insert_columns(self, columns, **kwargs) -> None:
        pass

    def validate_create_table_config(self) -> None:
        if self.Table.exists():
            # Checking compliance of types in the table and in the notebook.
            name_and_dtype_columns = {
                c[0]: c[1]
                for c in self.Table.describe()
                if c[2] not in ("MATERIALIZED", "ALIAS")
            }
            for schema_col in self.create_table_config["columns"]:
                schema_col = schema_col.split(" ")
                name, dtype = schema_col[0], schema_col[1]
                dtype_in_table = name_and_dtype_columns.get(name)
                if dtype_in_table and dtype_in_table != dtype:
                    raise AssertionError(
                        "AssertionError: Column data type in notebook "
                        f"does not match in table ({dtype_in_table=} != {dtype=})."
                    )
        # TODO: проверять orders & partition, чтоб были идентичными

        if not self.create_table_config.get("partition"):
            if self.data_cleaning_mode == self.DataCleaningMode.partition:
                raise Exception("Not set 'partition' in table schema")

    def add_columns(self) -> None:
        self.Table.add_column(name="_inserted", type="DateTime", expr="DEFAULT now()")

        # TODO: test
        col_names_in_table = {
            c[0] for c in self.Table.describe() if c[2] not in ("MATERIALIZED", "ALIAS")
        }
        for i, schema_col in enumerate(self.create_table_config["columns"]):
            name = schema_col.split(" ")[0]
            if name not in col_names_in_table:
                after_schema_col = self.create_table_config["columns"][i - 1]
                after_column_name = after_schema_col.split(" ")[0]
                query = f"ALTER TABLE {self.Table} ADD COLUMN {schema_col} AFTER {after_column_name}"
                self.client.execute(query)

    def create_table(self, create_table_config: dict) -> None:
        self.validate_create_table_config()
        self.client.create_table_mergetree(**create_table_config)
        self.add_columns()

    def clear_stale_data(self) -> None:
        if self.data_cleaning_mode == self.DataCleaningMode.truncate:
            self.logger.info("Truncate table")
            self.Table.truncate()

        elif self.data_cleaning_mode == self.DataCleaningMode.partition:
            p = list(set(self.partitions))
            self.logger.info(f"Drop partitions: {p}")
            self.Table.drop_partitions(p)
            self.partitions.clear()

        elif self.data_cleaning_mode == self.DataCleaningMode.off:
            pass

        else:
            raise KeyError(f"'Unknown {self.data_cleaning_mode=}'")

    def __call__(self, context: "TransformContext", *args, **kwargs) -> None:
        self.validate_insert_columns(context.insert_columns)

        if self.StageTable is None:
            raise Exception("Call through the context manager")

        if context.data:
            self.partitions += context.partitions

            rows = len(context.data[0])
            rows_before = self.StageTable.get_count_rows()
            self.logger.info(
                "Iteration of inserting data into a staging table"
                f"'{self.StageTable.db}.{self.StageTable.table}'"
            )
            self.StageTable.insert(
                context.data, context.insert_columns, types_check=True, columnar=True
            )
            rows_after = self.StageTable.get_count_rows()

            if not rows_after - rows_before == rows:
                raise AssertionError("Not all rows could be inserted")
        else:
            self.logger.info("Not data for insert")

    def __enter__(self) -> "ClickhouseLoader":
        stage_table_name = "{}_{}".format(
            self.create_table_config["table"], random.getrandbits(32)
        )
        self.create_table(self.create_table_config)
        self.StageTable = self.Table.copy_table(
            self.Table.db, stage_table_name, return_new_table=True
        )

        for sql in self.sql_before:
            self.client.execute(sql)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        is_identic = True
        try:
            if not exc_val:
                self.logger.info("Clear stale data")
                self.clear_stale_data()
                self.logger.info(
                    "Transferring data from a staging table "
                    f"'{self.StageTable.db}.{self.StageTable.table}'"
                )
                is_identic = self.Table.copy_data_from(
                    self.StageTable.db, self.StageTable.table
                )

        finally:
            self.StageTable.drop_table()
            self.StageTable = None

            if exc_type:
                raise
            elif not is_identic:
                self.logger.warning("AssertionError: Not all rows could be inserted")

        for sql in self.sql_after:
            self.client.execute(sql)
