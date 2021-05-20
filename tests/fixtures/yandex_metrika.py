from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseLoadPolicy
from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseTransformPolicy
from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader
from flowmaster.operators.etl.loaders.csv.service import CSVLoader
from flowmaster.operators.etl.policy import ETLFlowConfig
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
from tests.fixtures import work_policy, csv_load_policy, csv_transform_policy

yml_visits_to_csv_config = ETLFlowConfig(
    name="ymlogs_to_csv",
    provider=YandexMetrikaLogsProvider.name,
    storage=CSVLoader.name,
    work=work_policy,
    export=YandexMetrikaLogsExportPolicy(
        credentials=YandexMetrikaLogsExportPolicy.CredentialsPolicy(
            counter_id=0, access_token="token"
        ),
        params=YandexMetrikaLogsExportPolicy.ParamsPolicy(
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
    transform=csv_transform_policy,
    load=csv_load_policy,
)

yml_visits_to_clickhouse_config = ETLFlowConfig(
    name="ymlogs_to_clickhouse",
    provider=YandexMetrikaLogsProvider.name,
    storage=ClickhouseLoader.name,
    work=work_policy,
    export=YandexMetrikaLogsExportPolicy(
        credentials=YandexMetrikaLogsExportPolicy.CredentialsPolicy(
            counter_id=0, access_token="token"
        ),
        params=YandexMetrikaLogsExportPolicy.ParamsPolicy(
            source="visits", columns=[""]
        ),
    ),
    transform=ClickhouseTransformPolicy(
        error_policy="default",
        partition_columns=["Date"],
        column_map={"date": "Date"},
    ),
    load=ClickhouseLoadPolicy(
        credentials=ClickhouseLoadPolicy.CredentialsPolicy(
            user="user1", host="localhost"
        ),
        table_schema=ClickhouseLoadPolicy.TableSchemaPolicy(
            db="default",
            table="test_masterflow",
            columns=["Date Date"],
            orders=["Date"],
            partition=["Date"],
        ),
        data_cleaning_mode=ClickhouseLoader.DataCleaningMode.off,
        sql_before=["SELECT 1"],
        sql_after=["SELECT 2"],
    ),
)

ymm_goals_to_csv_config = ETLFlowConfig(
    name="ymm_goals_to_csv_config",
    provider=YandexMetrikaManagementProvider.name,
    storage=CSVLoader.name,
    work=work_policy,
    export=YandexMetrikaManagementExportPolicy(
        resource=YandexMetrikaManagementExport.ResourceNames.goals,
        credentials=YandexMetrikaManagementExportPolicy.CredentialsPolicy(
            access_token="token"
        ),
    ),
    transform=csv_transform_policy,
    load=csv_load_policy,
)

ymm_counters_to_csv_config = ETLFlowConfig(**ymm_goals_to_csv_config.dict())
ymm_counters_to_csv_config.export = YandexMetrikaManagementExportPolicy(
    resource=YandexMetrikaManagementExport.ResourceNames.counters,
    credentials=YandexMetrikaManagementExportPolicy.CredentialsPolicy(
        access_token="token"
    ),
)

ymm_clients_to_csv_config = ETLFlowConfig(**ymm_goals_to_csv_config.dict())
ymm_clients_to_csv_config.export = YandexMetrikaManagementExportPolicy(
    resource=YandexMetrikaManagementExport.ResourceNames.clients,
    credentials=YandexMetrikaManagementExportPolicy.CredentialsPolicy(
        access_token="token"
    ),
)

ymstats_to_csv_config = ETLFlowConfig(
    name="ymstats_to_csv_config",
    provider=YandexMetrikaStatsProvider.name,
    storage=CSVLoader.name,
    work=work_policy,
    export=YandexMetrikaStatsExportPolicy(
        credentials=YandexMetrikaStatsExportPolicy.CredentialsPolicy(
            access_token="token"
        ),
        params=YandexMetrikaStatsExportPolicy.ParamsPolicy(
            ids=0,
            metrics=["ym:s:visits"],
            date1=True,
            date2=True,
        ),
    ),
    transform=csv_transform_policy,
    load=csv_load_policy,
)
