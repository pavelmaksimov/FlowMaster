import datetime as dt

from flowmaster.operators.etl.policy import ETLFlowConfig
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir
from tests.fixtures.yandex_metrika import (
    ymm_goals_to_csv_config,
    ymm_counters_to_csv_config,
    ymm_clients_to_csv_config,
    ymstats_to_csv_config,
    yml_visits_to_csv_config,
)

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")


def test_management_clients():
    ymm_clients_to_csv_config.export.credentials = credentials[
        "yandex-metrika-management"
    ]
    config = ETLFlowConfig(**dict(ymm_clients_to_csv_config))
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))


def test_management_goals():
    ymm_goals_to_csv_config.export.credentials = credentials[
        "yandex-metrika-management"
    ]
    config = ETLFlowConfig(**dict(ymm_goals_to_csv_config))
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))


def test_management_counters():
    ymm_counters_to_csv_config.export.credentials = credentials[
        "yandex-metrika-management"
    ]
    config = ETLFlowConfig(**dict(ymm_counters_to_csv_config))
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))


def test_stats():
    ymstats_to_csv_config.export.credentials = credentials["yandex-metrika-stats"][
        "credentials"
    ]
    ymstats_to_csv_config.export.params = {
        **dict(ymstats_to_csv_config.export.params),
        **credentials["yandex-metrika-stats"]["params"],
    }
    config = ETLFlowConfig(**dict(ymstats_to_csv_config))
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))


def test_logs():
    yml_visits_to_csv_config.export.credentials = credentials["yandex-metrika-logs"]
    yml_visits_to_csv_config.export.params.columns = ["ym:s:bounce"]
    config = ETLFlowConfig(**dict(yml_visits_to_csv_config))
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1), dry_run=True))
