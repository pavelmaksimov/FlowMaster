from flowmaster.operators.etl.config import ETLFlowConfig
from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseLoadPolicy
from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseTransformPolicy
from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoad
from flowmaster.operators.etl.loaders.file.service import FileLoad
from flowmaster.operators.etl.providers import YandexDirectProvider
from flowmaster.operators.etl.providers.yandex_direct.policy import (
    YandexDirectExportPolicy as ExportPolicy,
)
from tests.fixtures import work_policy, file_load_policy, file_transform_policy

ya_direct_report_to_file_config = ETLFlowConfig(
    name="ya_direct_report_to_file",
    provider=YandexDirectProvider.name,
    storage=FileLoad.name,
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
    transform=file_transform_policy,
    load=file_load_policy,
)

ya_direct_report_to_clickhouse_config = ETLFlowConfig(
    **{
        **ya_direct_report_to_file_config.dict(),
        **dict(
            name="ya_direct_report_to_clickhouse_config",
            storage=ClickhouseLoad.name,
            transform=ClickhouseTransformPolicy(
                error_policy="default",
                column_map={"CampaignType": "CampaignType", "Cost": "Cost"},
            ),
            load=ClickhouseLoadPolicy(
                credentials=ClickhouseLoadPolicy.Credentials(
                    user="user1", host="localhost"
                ),
                table_schema=ClickhouseLoadPolicy.TableSchema(
                    db="default",
                    table="flowmaster_ya_direct_report_to_clickhouse_config",
                    columns=["CampaignType String", "Cost Float32"],
                    orders=["CampaignType"],
                ),
                data_cleaning_mode=ClickhouseLoad.DataCleaningMode.truncate,
                sql_before=["SELECT 1"],
                sql_after=["SELECT 2"],
            ),
        ),
    }
)

ya_direct_campaigns_to_file_config = ETLFlowConfig(
    **{
        **ya_direct_report_to_file_config.dict(),
        **dict(
            name="ya_direct_campaigns_to_file_config",
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

ya_direct_campaigns_to_clickhouse_config = ETLFlowConfig(
    **{
        **ya_direct_campaigns_to_file_config.dict(),
        **dict(
            name="ya_direct_campaigns_to_clickhouse_config",
            storage=ClickhouseLoad.name,
            transform=ClickhouseTransformPolicy(
                error_policy="default",
                column_map={"Id": "CampaignID", "Name": "CampaignName"},
            ),
            load=ClickhouseLoadPolicy(
                credentials=ClickhouseLoadPolicy.Credentials(
                    user="user1", host="localhost"
                ),
                table_schema=ClickhouseLoadPolicy.TableSchema(
                    db="default",
                    table="flowmaster_test_ya_direct_campaigns_to_clickhouse_config",
                    columns=["CampaignName String", "CampaignID UInt64"],
                    orders=["CampaignID"],
                ),
                data_cleaning_mode=ClickhouseLoad.DataCleaningMode.truncate,
                sql_before=["SELECT 1"],
                sql_after=["SELECT 2"],
            ),
        ),
    }
)
