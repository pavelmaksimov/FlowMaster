from flowmaster.operators.etl.config import ETLFlowConfig
from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseLoadPolicy
from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoad
from flowmaster.operators.etl.loaders.file.service import FileLoad
from flowmaster.operators.etl.providers import YandexMetrikaLogsProvider
from flowmaster.operators.etl.providers.yandex_metrika_logs import (
    YandexMetrikaLogsExportPolicy,
)
from flowmaster.operators.etl.providers.yandex_metrika_management import (
    YandexMetrikaManagementProvider,
)
from flowmaster.operators.etl.providers.yandex_metrika_management.export import (
    YandexMetrikaManagementExport,
)
from flowmaster.operators.etl.providers.yandex_metrika_management.policy import (
    BaseExportPolicy as YandexMetrikaManagementExportPolicy,
)
from flowmaster.operators.etl.providers.yandex_metrika_stats import (
    YandexMetrikaStatsExportPolicy,
    YandexMetrikaStatsProvider,
)
from flowmaster.operators.etl.transform.policy import ClickhouseTransformPolicy
from tests.fixtures import work_policy, file_load_policy, file_transform_policy

yml_visits_to_file_config = ETLFlowConfig(
    name="ymlogs_to_file",
    provider=YandexMetrikaLogsProvider.name,
    storage=FileLoad.name,
    work=work_policy,
    export=YandexMetrikaLogsExportPolicy(
        credentials=YandexMetrikaLogsExportPolicy.Credentials(
            counter_id=0, access_token="token"
        ),
        params=YandexMetrikaLogsExportPolicy.Params(
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
    ),
    transform=file_transform_policy,
    load=file_load_policy,
)

yml_visits_to_clickhouse_config = ETLFlowConfig(
    name="ymlogs_to_clickhouse",
    provider=YandexMetrikaLogsProvider.name,
    storage=ClickhouseLoad.name,
    work=work_policy,
    export=YandexMetrikaLogsExportPolicy(
        credentials=YandexMetrikaLogsExportPolicy.Credentials(
            counter_id=0, access_token="token"
        ),
        params=YandexMetrikaLogsExportPolicy.Params(source="visits", columns=[""]),
    ),
    transform=ClickhouseTransformPolicy(
        error_policy="default",
        partition_columns=["Date"],
        column_map={"date": "Date"},
    ),
    load=ClickhouseLoadPolicy(
        credentials=ClickhouseLoadPolicy.Credentials(user="user1", host="localhost"),
        table_schema=ClickhouseLoadPolicy.TableSchema(
            db="default",
            table="test_masterflow",
            columns=["Date Date"],
            orders=["Date"],
            partition=["Date"],
        ),
        data_cleaning_mode=ClickhouseLoad.DataCleaningMode.off,
        sql_before=["SELECT 1"],
        sql_after=["SELECT 2"],
    ),
)

ymm_goals_to_file_config = ETLFlowConfig(
    name="ymm_goals_to_file_config",
    provider=YandexMetrikaManagementProvider.name,
    storage=FileLoad.name,
    work=work_policy,
    export=YandexMetrikaManagementExportPolicy(
        resource=YandexMetrikaManagementExport.ResourceNames.goals,
        credentials=YandexMetrikaManagementExportPolicy.Credentials(
            access_token="token"
        ),
    ),
    transform=file_transform_policy,
    load=file_load_policy,
)

ymm_counters_to_file_config = ETLFlowConfig(**ymm_goals_to_file_config.dict())
ymm_counters_to_file_config.export = YandexMetrikaManagementExportPolicy(
    resource=YandexMetrikaManagementExport.ResourceNames.counters,
    credentials=YandexMetrikaManagementExportPolicy.Credentials(access_token="token"),
)

ymm_clients_to_file_config = ETLFlowConfig(**ymm_goals_to_file_config.dict())
ymm_clients_to_file_config.export = YandexMetrikaManagementExportPolicy(
    resource=YandexMetrikaManagementExport.ResourceNames.clients,
    credentials=YandexMetrikaManagementExportPolicy.Credentials(access_token="token"),
)

ymstats_to_file_config = ETLFlowConfig(
    name="ymstats_to_file_config",
    provider=YandexMetrikaStatsProvider.name,
    storage=FileLoad.name,
    work=work_policy,
    export=YandexMetrikaStatsExportPolicy(
        credentials=YandexMetrikaStatsExportPolicy.Credentials(access_token="token"),
        params=YandexMetrikaStatsExportPolicy.Params(
            ids=0,
            metrics=["ym:s:visits"],
            date1=True,
            date2=True,
        ),
    ),
    transform=file_transform_policy,
    load=file_load_policy,
)
