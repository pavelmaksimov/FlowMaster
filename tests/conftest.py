import datetime as dt

import pytest

from flowmaster.operators.etl.loaders.csv.policy import CSVLoadPolicy
from flowmaster.operators.etl.loaders.csv.policy import (
    CSVTransformPolicy,
)
from flowmaster.operators.etl.loaders.csv.service import CSVLoader
from flowmaster.operators.etl.policy import ETLFlowConfig
from flowmaster.operators.etl.providers import CSVProvider
from flowmaster.operators.etl.providers.csv.policy import CSVExportPolicy
from flowmaster.operators.etl.providers.sqlite import SQLiteProvider
from flowmaster.operators.etl.providers.sqlite.policy import SQLiteExportPolicy


@pytest.fixture()
def work_policy():
    return ETLFlowConfig.WorkPolicy(
        triggers=ETLFlowConfig.WorkPolicy.TriggersPolicy(
            schedule=ETLFlowConfig.WorkPolicy.TriggersPolicy.SchedulePolicy(
                timezone="Europe/Moscow",
                start_time="00:00:00",
                from_date=dt.date.today() - dt.timedelta(5),
                interval="daily",
            )
        )
    )


@pytest.fixture()
def csv_load_policy(tmp_path):
    return CSVLoadPolicy(
        path=str(tmp_path), file_name="csv_load_policy.csv", save_mode="w"
    )


@pytest.fixture()
def csv_transform_policy():
    return CSVTransformPolicy(error_policy="default")


@pytest.fixture()
def csv_export_policy(tmp_path):
    export_filepath = tmp_path / "export_data.csv"
    with open(export_filepath, "w") as file:
        file.write(
            "begin-miss-rows\n"
            "col1\tcol2\n"
            "1\n"
            "\n"
            "1\t2\n"
            "1\t2\t3\n"
            "1\t2\n"
            "1\t2\n"
        )
    return CSVExportPolicy(
        file_path=str(export_filepath),
        with_columns=True,
        columns=["col1", "col2"],
        chunk_size=1,
        skip_begin_lines=1,
    )


@pytest.fixture()
def config_csv_to_csv_with_columns(work_policy, csv_transform_policy, csv_export_policy, csv_load_policy):

    return ETLFlowConfig(
        name="csv_to_csv_with_columns",
        provider=CSVProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=csv_export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )


@pytest.fixture()
def config_csv_to_csv_without_columns(tmp_path, config_csv_to_csv_with_columns):
    with open(config_csv_to_csv_with_columns.export.file_path, "w") as file:
        # fmt: off
        file.write(
            "begin-miss-rows\n" 
            "1\n" 
            "\n" 
            "1\t2\n" 
            "1\t2\t3\n" 
            "1\t2\n" 
            "1\t2\n"
        )

    config_csv_to_csv_with_columns.name = "csv_to_csv_without_columns"
    config_csv_to_csv_with_columns.export.with_columns = False

    return config_csv_to_csv_with_columns


@pytest.fixture()
def sqlite_export_policy(tmp_path):
    db_path = tmp_path / 'test_sqlite_export.db'

    return SQLiteExportPolicy(
        db_path=str(db_path),
        table="test_export",
        columns=["id", "key"],
        sql_before=[
            "CREATE TABLE IF NOT EXISTS 'test_export' ('id' INTEGER NOT NULL PRIMARY KEY, 'key' TEXT NOT NULL);",
            "INSERT INTO test_export VALUES (1, 'one')",
            "INSERT INTO test_export VALUES (2, 'two')",
            "INSERT INTO test_export VALUES (3, 'three')",
        ],
        sql_after=["SELECT 2"],
        chunk_size=1,
        where="id != 3",
        order_by="id",
    )


@pytest.fixture()
def sqlite_to_csv_config(tmp_path, work_policy, sqlite_export_policy, csv_transform_policy, csv_load_policy):
    return ETLFlowConfig(
        name="sqlite_to_csv",
        provider=SQLiteProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=sqlite_export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
