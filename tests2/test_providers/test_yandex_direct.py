import datetime as dt

from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir
from tests.fixtures.yandex_direct import (
    ya_direct_report_to_csv_notebook,
    ya_direct_report_to_clickhouse_notebook,
    ya_direct_campaigns_to_csv_notebook,
    ya_direct_campaigns_to_clickhouse_notebook,
)

credentials_file = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")
yandex_direct_credentials = credentials_file["yandex-direct"]["credentials"]
clickhouse_credentials = credentials_file["clickhouse"]


def test_reports_to_csv():
    ya_direct_report_to_csv_notebook.export.credentials = yandex_direct_credentials
    notebook = ETLNotebook(**ya_direct_report_to_csv_notebook.dict())
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2))


def test_reports_to_clickhouse():
    ya_direct_report_to_clickhouse_notebook.export.credentials = (
        yandex_direct_credentials
    )
    ya_direct_report_to_clickhouse_notebook.load.credentials = clickhouse_credentials
    notebook = ETLNotebook(**ya_direct_report_to_clickhouse_notebook.dict())
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2))


def test_attributes_to_csv():
    ya_direct_campaigns_to_csv_notebook.export.credentials = yandex_direct_credentials
    notebook = ETLNotebook(**ya_direct_campaigns_to_csv_notebook.dict())
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2))


def test_attributes_to_clickhouse():
    ya_direct_campaigns_to_clickhouse_notebook.export.credentials = (
        yandex_direct_credentials
    )
    ya_direct_campaigns_to_clickhouse_notebook.load.credentials = clickhouse_credentials
    notebook = ETLNotebook(**ya_direct_campaigns_to_clickhouse_notebook.dict())
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2))
