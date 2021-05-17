import datetime as dt

import pytest

from flowmaster.operators.etl.loaders.file.policy import (
    FileLoadPolicy,
    FileTransformPolicy,
)
from flowmaster.operators.etl.loaders.file.service import FileLoad
from flowmaster.operators.etl.policy import ETLFlowConfig
from flowmaster.operators.etl.providers import CSVProvider
from flowmaster.operators.etl.providers.csv.policy import CSVExportPolicy


@pytest.fixture()
def work_policy():
    return ETLFlowConfig.WorkPolicy(
        schedule=ETLFlowConfig.WorkPolicy.SchedulePolicy(
            timezone="Europe/Moscow",
            start_time="00:00:00",
            from_date=dt.date.today() - dt.timedelta(5),
            interval="daily",
        )
    )


@pytest.fixture()
def csv_load_policy(tmp_path):
    return FileLoadPolicy(
        path=str(tmp_path), file_name="csv_load_policy.csv", save_mode="w"
    )


@pytest.fixture()
def csv_transform_policy():
    return FileTransformPolicy(error_policy="default")


@pytest.fixture()
def config_csv_to_csv_with_columns(tmp_path, work_policy, csv_transform_policy):
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

    csv_load_policy = FileLoadPolicy(
        path=str(tmp_path), file_name="load_data.csv", save_mode="w"
    )

    return ETLFlowConfig(
        name="csv_to_csv_with_columns",
        provider=CSVProvider.name,
        storage=FileLoad.name,
        work=work_policy,
        export=CSVExportPolicy(
            file_path=str(export_filepath),
            with_columns=True,
            columns=["col1", "col2"],
            chunk_size=1,
            skip_begin_lines=1,
        ),
        transform=csv_transform_policy,
        load=csv_load_policy,
    )


@pytest.fixture()
def config_csv_to_csv_without_columns(tmp_path, config_csv_to_csv_with_columns):
    with open(config_csv_to_csv_with_columns.export.file_path, "w") as file:
        file.write(
            "begin-miss-rows\n" "1\n" "\n" "1\t2\n" "1\t2\t3\n" "1\t2\n" "1\t2\n"
        )

    config_csv_to_csv_with_columns.name = "csv_to_csv_without_columns"
    config_csv_to_csv_with_columns.export.with_columns = False

    return config_csv_to_csv_with_columns
