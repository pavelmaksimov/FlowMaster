import datetime as dt

from tests.fixtures.yandex_metrika import (
    ymm_goals_to_csv_notebook,
    ymm_counters_to_csv_notebook,
    ymm_clients_to_csv_notebook,
    ymstats_to_csv_notebook,
    ya_metrika_logs_to_csv_notebook,
)

from flowmaster.operators.etl.core import ETLOperator
from flowmaster.operators.etl.policy import ETLNotebookPolicy
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")


def test_management_clients():
    ymm_clients_to_csv_notebook.export.credentials = credentials[
        "yandex-metrika-management"
    ]
    notebook = ETLNotebookPolicy(**dict(ymm_clients_to_csv_notebook))
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))


def test_management_goals():
    ymm_goals_to_csv_notebook.export.credentials = credentials[
        "yandex-metrika-management"
    ]
    notebook = ETLNotebookPolicy(**dict(ymm_goals_to_csv_notebook))
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))


def test_management_counters():
    ymm_counters_to_csv_notebook.export.credentials = credentials[
        "yandex-metrika-management"
    ]
    notebook = ETLNotebookPolicy(**dict(ymm_counters_to_csv_notebook))
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))


def test_stats():
    ymstats_to_csv_notebook.export.credentials = credentials["yandex-metrika-stats"][
        "credentials"
    ]
    ymstats_to_csv_notebook.export.params = {
        **dict(ymstats_to_csv_notebook.export.params),
        **credentials["yandex-metrika-stats"]["params"],
    }
    notebook = ETLNotebookPolicy(**dict(ymstats_to_csv_notebook))
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))


def test_logs():
    ya_metrika_logs_to_csv_notebook.export.credentials = credentials[
        "yandex-metrika-logs"
    ]
    ya_metrika_logs_to_csv_notebook.export.params.columns = ["ym:s:bounce"]
    notebook = ETLNotebookPolicy(**dict(ya_metrika_logs_to_csv_notebook))
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1), dry_run=True))
