import datetime as dt
import os
import uuid
from pathlib import Path

import pendulum
import pytest


@pytest.fixture(autouse=True)
def preparation_for_tests(tmp_path):
    path = tmp_path.parent / "FlowMaster"
    os.environ["FLOWMASTER_HOME"] = str(path)

    from flowmaster import create_initial_dirs_and_files

    create_initial_dirs_and_files()


@pytest.fixture()
def pendulum_utctoday():
    now = pendulum.datetime(2021, 1, 1, tz="UTC")
    pendulum.set_test_now(now)
    yield now
    pendulum.set_test_now(now)


@pytest.fixture()
def pendulum_utcnow():
    now = pendulum.datetime(2021, 1, 1, 10, 10, 10, tz="UTC")
    pendulum.set_test_now(now)
    yield now
    pendulum.set_test_now(now)


@pytest.fixture()
def pools():
    from flowmaster.pool import pools

    return pools


def tests_dir() -> Path:
    cwd = Path.cwd()
    if "flowmaster-dev" in cwd.parts:
        parts_to_fm = cwd.parts[: cwd.parts.index("flowmaster-dev") + 1]
        return Path(*parts_to_fm) / "tests"
    elif "flowmaster-dev" in cwd.iterdir():
        return cwd / "flowmaster-dev" / "tests"
    else:
        raise FileNotFoundError


@pytest.fixture()
def flowitem_model():
    from flowmaster.models import FlowItem

    FlowItem.name_for_test = "__fm_test__"
    FlowItem.clear("__fm_test__")

    return FlowItem


@pytest.fixture()
def flowitem(pendulum_utctoday, flowitem_model):
    name = str(uuid.uuid4())
    flowitem_model.clear(name, from_time=pendulum_utctoday, to_time=pendulum_utctoday)

    yield flowitem_model.create(
        **{
            flowitem_model.name.name: name,
            flowitem_model.worktime.name: pendulum_utctoday,
        }
    )


@pytest.fixture()
def work_policy(pendulum_utcnow):
    from flowmaster.operators.etl.policy import ETLNotebookPolicy

    return ETLNotebookPolicy.WorkPolicy(
        triggers=ETLNotebookPolicy.WorkPolicy.TriggersPolicy(
            schedule=ETLNotebookPolicy.WorkPolicy.TriggersPolicy.SchedulePolicy(
                timezone="Europe/Moscow",
                start_time="00:00:00",
                from_date=pendulum.today() - dt.timedelta(5),
                interval="daily",
            )
        )
    )


@pytest.fixture()
def seconds_interval_work_policy(pendulum_utcnow):
    from flowmaster.operators.etl.policy import ETLNotebookPolicy

    return ETLNotebookPolicy.WorkPolicy(
        triggers=ETLNotebookPolicy.WorkPolicy.TriggersPolicy(
            schedule=ETLNotebookPolicy.WorkPolicy.TriggersPolicy.SchedulePolicy(
                timezone="Europe/Moscow",
                start_time="00:00:00",
                from_date=None,
                interval=60,
            )
        )
    )


@pytest.fixture()
def csv_load_policy(tmp_path):
    from flowmaster.operators.etl.loaders.csv.policy import CSVLoadPolicy

    name = uuid.uuid4()

    return CSVLoadPolicy(path=str(tmp_path), file_name=f"{name}.csv", save_mode="w")


@pytest.fixture()
def csv_transform_policy():
    from flowmaster.operators.etl.loaders.csv.policy import CSVTransformPolicy

    return CSVTransformPolicy(error_policy="default")


@pytest.fixture()
def csv_export_policy(tmp_path):
    from flowmaster.operators.etl.providers import CSVProvider

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

    return CSVProvider.policy_model(
        file_path=str(export_filepath),
        with_columns=True,
        columns=["col1", "col2"],
        chunk_size=1,
        skip_begin_lines=1,
    )


@pytest.fixture()
def csv_to_csv_with_columns_notebook(
    work_policy,
    csv_transform_policy,
    csv_export_policy,
    csv_load_policy,
    flowitem_model,
):
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.providers.csv import CSVProvider

    name = "__test_csv_to_csv_with_columns__"
    flowitem_model.clear(name)
    yield ETLNotebookPolicy(
        name=name,
        provider=CSVProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=csv_export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)


@pytest.fixture()
def csv_to_csv_without_columns_notebook(
    csv_to_csv_with_columns_notebook,
    flowitem_model,
):
    with open(csv_to_csv_with_columns_notebook.export.file_path, "w") as file:
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
        # fmt: on

    name = "__test_csv_to_csv_with_columns__"
    n = csv_to_csv_with_columns_notebook.copy(deep=True, update={"name": name})
    n.export.with_columns = False
    flowitem_model.clear(name)
    yield n
    flowitem_model.clear(name)


@pytest.fixture()
def sqlite_export_policy(tmp_path):
    from flowmaster.operators.etl.providers import SQLiteProvider

    db_path = tmp_path / "test_sqlite_export.db"

    return SQLiteProvider.policy_model(
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
def sqlite_to_csv_notebook(
    work_policy,
    sqlite_export_policy,
    csv_transform_policy,
    csv_load_policy,
    flowitem_model,
):
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.providers.sqlite import SQLiteProvider

    name = "__test_sqlite_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebookPolicy(
        name=name,
        provider=SQLiteProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=sqlite_export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)


@pytest.fixture()
def flowmasterdata_items_export_policy():
    from flowmaster.operators.etl.providers.flowmaster_data import (
        FlowmasterDataProvider,
    )

    return FlowmasterDataProvider.policy_model(
        resource="items",
        columns=[
            "name",
            "worktime",
            "status",
            "data",
            "notebook_hash",
            "retries",
            "duration",
            "info",
            "started_utc",
            "finished_utc",
            "created_utc",
            "updated_utc",
        ],
        export_mode="all",
    )


@pytest.fixture()
def flowmasterdata_pools_export_policy():
    from flowmaster.operators.etl.providers.flowmaster_data import (
        FlowmasterDataProvider,
    )

    return FlowmasterDataProvider.policy_model(
        resource="pools",
        columns=["name", "size", "limit", "datetime"],
        export_mode="all",
    )


@pytest.fixture()
def flowmasterdata_queues_export_policy():
    from flowmaster.operators.etl.providers.flowmaster_data import (
        FlowmasterDataProvider,
    )

    return FlowmasterDataProvider.policy_model(
        resource="queues", columns=["name", "size", "datetime"], export_mode="all"
    )


@pytest.fixture()
def flowmasterdata_items_to_csv_notebook(
    tmp_path,
    seconds_interval_work_policy,
    flowmasterdata_items_export_policy,
    csv_transform_policy,
    csv_load_policy,
    flowitem_model,
):
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.providers.flowmaster_data import (
        FlowmasterDataProvider,
    )

    name = "__test_flowmasterdata_items_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebookPolicy(
        name=name,
        provider=FlowmasterDataProvider.name,
        storage=CSVLoader.name,
        work=seconds_interval_work_policy,
        export=flowmasterdata_items_export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)


@pytest.fixture()
def criteo_export_policy():
    from flowmaster.operators.etl.providers.criteo import CriteoProvider

    return CriteoProvider.policy_model(
        api_version="202104",
        credentials=CriteoProvider.policy_model.CredentialsPolicy(
            client_id="client_id", client_secret="client_secret"
        ),
        resource="stats",
        params=CriteoProvider.policy_model.StatsV202104ParamsPolicy(
            dimensions=["Day"],
            metrics=["Clicks"],
            currency="RUB",
            timezone="Europe/Moscow",
        ),
        chunk_size=100,
        concurrency=1,
    )


@pytest.fixture()
def criteo_to_csv_notebook(
    tmp_path,
    work_policy,
    criteo_export_policy,
    csv_transform_policy,
    csv_load_policy,
    flowitem_model,
):
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.providers.criteo import CriteoProvider

    name = "__test_criteo_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebookPolicy(
        name=name,
        provider=CriteoProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=criteo_export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)


@pytest.fixture()
def ya_metrika_logs_export_policy(tmp_path):
    from flowmaster.operators.etl.providers import YandexMetrikaLogsProvider

    return YandexMetrikaLogsProvider.policy_model(
        credentials=YandexMetrikaLogsProvider.policy_model.CredentialsPolicy(
            counter_id=0, access_token="token"
        ),
        params=YandexMetrikaLogsProvider.policy_model.ParamsPolicy(
            source="visits",
            columns=[
                "ym:s:counterID",
                "ym:s:clientID",
                "ym:s:visitID",
                "ym:s:date",
                "ym:s:dateTime",
                "ym:s:lastTrafficSource",
                "ym:s:startURL",
                "ym:s:pageViews",
            ],
        ),
    )


@pytest.fixture()
def ya_metrika_logs_to_csv_notebook(
    tmp_path,
    work_policy,
    ya_metrika_logs_export_policy,
    csv_transform_policy,
    csv_load_policy,
    flowitem_model,
):
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.providers import YandexMetrikaLogsProvider

    name = "__test_ya_metrika_logs_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebookPolicy(
        name=name,
        provider=YandexMetrikaLogsProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=ya_metrika_logs_export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)


# ===============================


@pytest.fixture()
def yml_visits_to_clickhouse_notebook(flowitem_model, ya_metrika_logs_to_csv_notebook):
    from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader

    name = "__test_ya_metrika_logs_to_csv__"
    n = ya_metrika_logs_to_csv_notebook.copy(
        deep=True,
        update=dict(
            name=name,
            storage=ClickhouseLoader.name,
            transform=ClickhouseLoader.transform_policy_model(
                error_policy="default",
                partition_columns=["Date"],
                column_map={"date": "Date"},
            ),
            load=ClickhouseLoader.policy_model(
                credentials=ClickhouseLoader.policy_model.CredentialsPolicy(
                    user="user1", host="localhost"
                ),
                table_schema=ClickhouseLoader.policy_model.TableSchemaPolicy(
                    db="default",
                    table="test_masterflow",
                    columns=[
                        "Date Date",
                    ],
                    orders=["Date"],
                    partition=["Date"],
                ),
                data_cleaning_mode=ClickhouseLoader.DataCleaningMode.off,
                sql_before=["SELECT 1"],
                sql_after=["SELECT 2"],
            ),
        ),
    )
    flowitem_model.clear(name)
    yield n
    flowitem_model.clear(name)


@pytest.fixture()
def ymm_goals_to_csv_notebook(flowitem_model, csv_transform_policy, csv_load_policy):
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.providers import YandexMetrikaManagementProvider

    name = "__test_ya_metrika_goals_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebookPolicy(
        name=name,
        provider=YandexMetrikaManagementProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=YandexMetrikaManagementProvider.policy_model(
            resource=YandexMetrikaManagementProvider.policy_model.ResourceNames.goals,
            credentials=YandexMetrikaManagementProvider.policy_model.CredentialsPolicy(
                access_token="token"
            ),
        ),
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)


@pytest.fixture()
def ymm_counters_to_csv_notebook(ymm_goals_to_csv_notebook):
    from flowmaster.operators.etl.providers import YandexMetrikaManagementProvider

    name = "__test_ya_metrika_counters_to_csv__"
    n = ymm_goals_to_csv_notebook.copy(
        deep=True,
        update=dict(
            export=YandexMetrikaManagementProvider.policy_model(
                resource=YandexMetrikaManagementProvider.policy_model.ResourceNames.counters,
                credentials=YandexMetrikaManagementProvider.policy_model.CredentialsPolicy(
                    access_token="token"
                ),
            )
        ),
    )
    flowitem_model.clear(name)
    yield n
    flowitem_model.clear(name)


@pytest.fixture()
def ymm_clients_to_csv_notebook(ymm_goals_to_csv_notebook):
    from flowmaster.operators.etl.providers import YandexMetrikaManagementProvider

    name = "__test_ya_metrika_clients_to_csv__"
    n = ymm_goals_to_csv_notebook.copy(
        deep=True,
        update=dict(
            export=YandexMetrikaManagementProvider.policy_model(
                resource=YandexMetrikaManagementProvider.policy_model.ResourceNames.clients,
                credentials=YandexMetrikaManagementProvider.policy_model.CredentialsPolicy(
                    access_token="token"
                ),
            )
        ),
    )
    flowitem_model.clear(name)
    yield n
    flowitem_model.clear(name)


@pytest.fixture()
def ymstats_to_csv_notebook(work_policy, csv_transform_policy, csv_load_policy):
    from flowmaster.operators.etl.providers import YandexMetrikaStatsProvider
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebookPolicy

    name = "__test_ya_metrika_stats_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebookPolicy(
        name=name,
        provider=YandexMetrikaStatsProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=YandexMetrikaStatsProvider.policy_model(
            credentials=YandexMetrikaStatsProvider.policy_model.CredentialsPolicy(
                access_token="token"
            ),
            params=YandexMetrikaStatsProvider.policy_model.ParamsPolicy(
                ids=0,
                metrics=["ym:s:visits"],
                date1=True,
                date2=True,
            ),
        ),
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)


@pytest.fixture()
def fakedata_to_csv_notebook(
    flowitem_model, work_policy, csv_transform_policy, csv_load_policy
):
    from flowmaster.operators.etl.providers import FakeDataProvider
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebookPolicy

    name = "__test_ya_metrika_stats_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebookPolicy(
        name=name,
        provider=FakeDataProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=FakeDataProvider.policy_model(rows=1),
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)
