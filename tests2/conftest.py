import uuid
from pathlib import Path

import pendulum
import pytest
import yaml


@pytest.fixture()
def pendulum_utcnow():
    now = pendulum.datetime(2021, 1, 1, 10, 10, 10, tz="UTC")
    pendulum.set_test_now(now)
    yield now
    pendulum.set_test_now(now)


@pytest.fixture()
def tests_dir() -> Path:
    cwd = Path.cwd()
    parts_to_fm = cwd.parts[: cwd.parts.index("flowmaster-dev") + 1]
    return Path(*parts_to_fm) / "tests2"


@pytest.fixture()
def credentials_dict(tests_dir):
    with open(tests_dir / "credentials.yml", "rb") as f:
        return yaml.full_load(f.read())


@pytest.fixture()
def clickhouse_credentials(credentials_dict):
    return credentials_dict["clickhouse"]


@pytest.fixture()
def postgres_credentials(credentials_dict):
    return credentials_dict["postgres"]


@pytest.fixture()
def yandex_direct_credentials(credentials_dict):
    return credentials_dict["yandex-direct"]["credentials"]


@pytest.fixture()
def flowitem_model():
    from flowmaster.models import FlowItem

    FlowItem.name_for_test = "__fm_test__"
    FlowItem.clear("__fm_test__")

    return FlowItem


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
def work_policy(pendulum_utcnow):
    from flowmaster.operators.etl.policy import ETLNotebook

    return ETLNotebook.WorkPolicy(
        triggers=ETLNotebook.WorkPolicy.TriggersPolicy(
            schedule=ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
                timezone="Europe/Moscow",
                start_time="00:00:00",
                from_date=pendulum.today().add(days=-5),
                interval="daily",
            )
        )
    )


@pytest.fixture()
def ya_direct_report_to_csv_notebook(
    flowitem_model,
    work_policy,
    csv_transform_policy,
    csv_load_policy,
    yandex_direct_credentials,
):
    from flowmaster.operators.etl.providers import YandexDirectProvider
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebook

    name = "__test2_ya_direct_report_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebook(
        name=name,
        provider=YandexDirectProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=YandexDirectProvider.policy_model(
            credentials=YandexDirectProvider.policy_model.CredentialsPolicy(
                **yandex_direct_credentials
            ),
            resource="reports",
            headers=YandexDirectProvider.policy_model.HeadersPolicy(
                return_money_in_micros=True
            ),
            body=YandexDirectProvider.policy_model.ReportBodyPolicy(
                params=YandexDirectProvider.policy_model.ReportBodyPolicy.ReportParamsPolicy(
                    ReportType="ACCOUNT_PERFORMANCE_REPORT",
                    DateRangeType="AUTO",
                    FieldNames=["CampaignType", "Cost"],
                    IncludeVAT="NO",
                    Page=YandexDirectProvider.policy_model.ReportBodyPolicy.ReportParamsPolicy.PagePolicy(
                        Limit=10
                    ),
                ),
            ),
        ),
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)


@pytest.fixture()
def ya_direct_report_to_clickhouse_notebook(
    flowitem_model, ya_direct_report_to_csv_notebook, clickhouse_credentials
):
    from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader

    name = "__test2_ya_direct_report_to_clickhouse__"
    n = ya_direct_report_to_csv_notebook.copy(
        deep=True,
        update=dict(
            name=name,
            storage=ClickhouseLoader.name,
            transform=ClickhouseLoader.transform_policy_model(
                error_policy="default",
                column_map={"CampaignType": "CampaignType", "Cost": "Cost"},
            ),
            load=ClickhouseLoader.policy_model(
                credentials=ClickhouseLoader.policy_model.CredentialsPolicy(
                    **clickhouse_credentials
                ),
                table_schema=ClickhouseLoader.policy_model.TableSchemaPolicy(
                    db="default",
                    table="flowmaster_ya_direct_report_to_clickhouse",
                    columns=["CampaignType String", "Cost Float32"],
                    orders=["CampaignType"],
                ),
                data_cleaning_mode=ClickhouseLoader.DataCleaningMode.truncate,
                sql_before=["SELECT 1"],
                sql_after=["SELECT 2"],
            ),
        ),
    )
    loader = ClickhouseLoader(n)
    loader.Table.drop_table()
    flowitem_model.clear(name)
    yield n
    flowitem_model.clear(name)
    loader.Table.drop_table()


@pytest.fixture()
def ya_direct_campaigns_to_csv_notebook(
    flowitem_model, ya_direct_report_to_csv_notebook
):
    from flowmaster.operators.etl.providers import YandexDirectProvider

    name = "__test2_ya_direct_campaigns_to_csv__"
    n = ya_direct_report_to_csv_notebook.copy(
        deep=True,
        update=dict(
            name=name,
            export=YandexDirectProvider.policy_model(
                credentials=ya_direct_report_to_csv_notebook.export.credentials,
                resource="campaigns",
                headers=YandexDirectProvider.policy_model.HeadersPolicy(
                    return_money_in_micros=True
                ),
                body=YandexDirectProvider.policy_model.BodyPolicy(
                    method="get",
                    params={
                        "SelectionCriteria": {},
                        "FieldNames": ["Id", "Name"],
                        "Page": {"Limit": 1},
                    },
                ),
            ),
        ),
    )
    flowitem_model.clear(name)
    yield n
    flowitem_model.clear(name)


@pytest.fixture()
def ya_direct_campaigns_to_clickhouse_notebook(
    flowitem_model, ya_direct_campaigns_to_csv_notebook, clickhouse_credentials
):
    from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader

    name = "__test2_ya_direct_campaigns_to_clickhouse__"
    n = ya_direct_campaigns_to_csv_notebook.copy(
        deep=True,
        update=dict(
            name=name,
            storage=ClickhouseLoader.name,
            transform=ClickhouseLoader.transform_policy_model(
                error_policy="default",
                column_map={"Id": "CampaignID", "Name": "CampaignName"},
            ),
            load=ClickhouseLoader.policy_model(
                credentials=ClickhouseLoader.policy_model.CredentialsPolicy(
                    **clickhouse_credentials
                ),
                table_schema=ClickhouseLoader.policy_model.TableSchemaPolicy(
                    db="default",
                    table="flowmaster_test_ya_direct_campaigns_to_clickhouse",
                    columns=["CampaignName String", "CampaignID UInt64"],
                    orders=["CampaignID"],
                ),
                data_cleaning_mode=ClickhouseLoader.DataCleaningMode.truncate,
                sql_before=["SELECT 1"],
                sql_after=["SELECT 2"],
            ),
        ),
    )
    loader = ClickhouseLoader(n)
    loader.Table.drop_table()
    flowitem_model.clear(name)
    yield n
    flowitem_model.clear(name)
    loader.Table.drop_table()


@pytest.fixture()
def postgres_export_policy(postgres_credentials):
    from flowmaster.operators.etl.providers import PostgresProvider

    return PostgresProvider.policy_model(
        table="test_table",
        columns=["id", "key"],
        sql_before=[
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER, key TEXT);",
            "TRUNCATE TABLE test_table",
            "INSERT INTO test_table VALUES (1, 'one')",
            "INSERT INTO test_table VALUES (2, 'two')",
            "INSERT INTO test_table VALUES (3, 'three')",
        ],
        sql_after=["SELECT 2"],
        chunk_size=1,
        where="id != 3",
        order_by="id DESC",
        **postgres_credentials
    )


@pytest.fixture()
def postgres_to_csv_notebook(
    work_policy,
    postgres_export_policy,
    csv_transform_policy,
    csv_load_policy,
    flowitem_model,
):
    from flowmaster.operators.etl.loaders.csv.service import CSVLoader
    from flowmaster.operators.etl.policy import ETLNotebook
    from flowmaster.operators.etl.providers.postgres import PostgresProvider

    name = "__test_postgres_to_csv__"
    flowitem_model.clear(name)
    yield ETLNotebook(
        name=name,
        provider=PostgresProvider.name,
        storage=CSVLoader.name,
        work=work_policy,
        export=postgres_export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    flowitem_model.clear(name)
