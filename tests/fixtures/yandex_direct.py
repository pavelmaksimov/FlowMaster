from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseLoadPolicy
from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseTransformPolicy
from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader
from flowmaster.operators.etl.loaders.csv.service import CSVLoader
from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.operators.etl.providers import YandexDirectProvider
from flowmaster.operators.etl.providers.yandex_direct.policy import (
    YandexDirectExportPolicy as ExportPolicy,
)
from tests.fixtures import work_policy, csv_load_policy, csv_transform_policy

ya_direct_report_to_csv_notebook = ETLNotebook(
    name="ya_direct_report_to_csv",
    provider=YandexDirectProvider.name,
    storage=CSVLoader.name,
    work=work_policy,
    export=ExportPolicy(
        credentials=ExportPolicy.CredentialsPolicy(access_token="token"),
        resource="reports",
        headers=ExportPolicy.HeadersPolicy(return_money_in_micros=True),
        body=ExportPolicy.ReportBodyPolicy(
            params=ExportPolicy.ReportBodyPolicy.ReportParamsPolicy(
                ReportType="ACCOUNT_PERFORMANCE_REPORT",
                DateRangeType="AUTO",
                FieldNames=["CampaignType", "Cost"],
                IncludeVAT="NO",
                Page=ExportPolicy.ReportBodyPolicy.ReportParamsPolicy.PagePolicy(
                    Limit=10
                ),
            ),
        ),
    ),
    transform=csv_transform_policy,
    load=csv_load_policy,
)

ya_direct_report_to_clickhouse_notebook = ETLNotebook(
    **{
        **ya_direct_report_to_csv_notebook.dict(),
        **dict(
            name="ya_direct_report_to_clickhouse_notebook",
            storage=ClickhouseLoader.name,
            transform=ClickhouseTransformPolicy(
                error_policy="default",
                column_map={"CampaignType": "CampaignType", "Cost": "Cost"},
            ),
            load=ClickhouseLoadPolicy(
                credentials=ClickhouseLoadPolicy.CredentialsPolicy(
                    user="user1", host="localhost"
                ),
                table_schema=ClickhouseLoadPolicy.TableSchemaPolicy(
                    db="default",
                    table="flowmaster_ya_direct_report_to_clickhouse_notebook",
                    columns=["CampaignType String", "Cost Float32"],
                    orders=["CampaignType"],
                ),
                data_cleaning_mode=ClickhouseLoader.DataCleaningMode.truncate,
                sql_before=["SELECT 1"],
                sql_after=["SELECT 2"],
            ),
        ),
    }
)

ya_direct_campaigns_to_csv_notebook = ETLNotebook(
    **{
        **ya_direct_report_to_csv_notebook.dict(),
        **dict(
            name="ya_direct_campaigns_to_csv",
            export=ExportPolicy(
                credentials=ExportPolicy.CredentialsPolicy(access_token="token"),
                resource="campaigns",
                headers=ExportPolicy.HeadersPolicy(return_money_in_micros=True),
                body=ExportPolicy.BodyPolicy(
                    method="get",
                    params={
                        "SelectionCriteria": {},
                        "FieldNames": ["Id", "Name"],
                        "Page": {"Limit": 1},
                    },
                ),
            ),
        ),
    }
)

ya_direct_campaigns_to_clickhouse_notebook = ETLNotebook(
    **{
        **ya_direct_campaigns_to_csv_notebook.dict(),
        **dict(
            name="ya_direct_campaigns_to_clickhouse",
            storage=ClickhouseLoader.name,
            transform=ClickhouseTransformPolicy(
                error_policy="default",
                column_map={"Id": "CampaignID", "Name": "CampaignName"},
            ),
            load=ClickhouseLoadPolicy(
                credentials=ClickhouseLoadPolicy.CredentialsPolicy(
                    user="user1", host="localhost"
                ),
                table_schema=ClickhouseLoadPolicy.TableSchemaPolicy(
                    db="default",
                    table="flowmaster_test_ya_direct_campaigns_to_clickhouse_notebook",
                    columns=["CampaignName String", "CampaignID UInt64"],
                    orders=["CampaignID"],
                ),
                data_cleaning_mode=ClickhouseLoader.DataCleaningMode.truncate,
                sql_before=["SELECT 1"],
                sql_after=["SELECT 2"],
            ),
        ),
    }
)
