import datetime as dt
from typing import Iterator

from mock import Mock

from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.operators.etl.providers.yandex_metrika_logs.export import (
    YandexMetrikaLogsExport,
)
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir
from tests.fixtures.yandex_metrika import (
    yml_visits_to_csv_notebook,
)

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")


def test_codex_telegram():
    def export_func(start_period, end_period) -> Iterator[tuple[dict, list, list]]:
        yield ({}, ["date"], [[start_period]])

    yml_visits_to_csv_notebook.work.notifications = ETLNotebook.WorkPolicy.NotificationsPolicy(
        codex_telegram=ETLNotebook.WorkPolicy.NotificationsPolicy.CodexTelegramPolicy(
            links=[credentials["codex_telegram"]],
            on_success=True,
        )
    )
    notebook = ETLNotebook(**dict(yml_visits_to_csv_notebook))

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)
    etl_flow = ETLOperator(notebook)

    list(
        etl_flow(
            start_period=dt.datetime(2021, 1, 1), end_period=dt.datetime(2021, 1, 1)
        )
    )
