import datetime as dt
from typing import Iterator

from mock import Mock

from flowmaster.operators.etl.providers.yandex_metrika_logs.export import (
    YandexMetrikaLogsExport,
)
from flowmaster.operators.etl.service import ETLOperator
from tests.fixtures.yandex_metrika import yml_visits_to_file_config


def test_flow():
    def export_func(start_date, end_date) -> Iterator[tuple[dict, list, list]]:
        yield ({}, ["col1"], [[start_date]])
        yield ({}, ["col1"], [[end_date]])

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)

    yml_visits_to_file_config.load.file_name = f"{test_flow.__name__}.tsv"
    yml_visits_to_file_config.load.with_columns = True

    flow = ETLOperator(yml_visits_to_file_config)

    list(flow(start_period=dt.datetime(2021, 1, 1), end_period=dt.datetime(2021, 1, 2)))
