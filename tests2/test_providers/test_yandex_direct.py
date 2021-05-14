import datetime as dt

from flowmaster.operators.etl.policy import ETLFlowConfig
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir
from tests.fixtures.yandex_direct import (
    ya_direct_report_to_file_config,
    ya_direct_report_to_clickhouse_config,
    ya_direct_campaigns_to_file_config,
    ya_direct_campaigns_to_clickhouse_config,
)

credentials_file = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")
yandex_direct_credentials = credentials_file["yandex-direct"]["credentials"]
clickhouse_credentials = credentials_file["clickhouse"]


def test_reports_to_file():
    ya_direct_report_to_file_config.export.credentials = yandex_direct_credentials
    config = ETLFlowConfig(**ya_direct_report_to_file_config.dict())
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2))


def test_reports_to_clickhouse():
    ya_direct_report_to_clickhouse_config.export.credentials = yandex_direct_credentials
    ya_direct_report_to_clickhouse_config.load.credentials = clickhouse_credentials
    config = ETLFlowConfig(**ya_direct_report_to_clickhouse_config.dict())
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2))


def test_attributes_to_file():
    ya_direct_campaigns_to_file_config.export.credentials = yandex_direct_credentials
    config = ETLFlowConfig(**ya_direct_campaigns_to_file_config.dict())
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2))


def test_attributes_to_clickhouse():
    ya_direct_campaigns_to_clickhouse_config.export.credentials = (
        yandex_direct_credentials
    )
    ya_direct_campaigns_to_clickhouse_config.load.credentials = clickhouse_credentials
    config = ETLFlowConfig(**ya_direct_campaigns_to_clickhouse_config.dict())
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2))
