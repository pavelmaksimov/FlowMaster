import datetime as dt
from typing import Iterator

from mock import Mock
from tests.fixtures.yandex_metrika import (
    ya_metrika_logs_to_csv_notebook,
)

from flowmaster.operators.etl.core import ETLOperator
from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.operators.etl.providers import Providers
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")


def test_codex_telegram():
    def export_func(start_period, end_period) -> Iterator[tuple[dict, list, list]]:
        yield ({}, ["date"], [[start_period]])

    ya_metrika_logs_to_csv_notebook.work.notifications = ETLNotebook.WorkPolicy.NotificationsPolicy(
        codex_telegram=ETLNotebook.WorkPolicy.NotificationsPolicy.CodexTelegramPolicy(
            links=[credentials["codex_telegram"]],
            on_success=True,
        )
    )
    notebook = ETLNotebook(**dict(ya_metrika_logs_to_csv_notebook))

    Providers.YandexMetrikaLogsProvider.export_class.__call__ = Mock(
        side_effect=export_func
    )
    etl_flow = ETLOperator(notebook)

    list(
        etl_flow(
            start_period=dt.datetime(2021, 1, 1), end_period=dt.datetime(2021, 1, 1)
        )
    )
