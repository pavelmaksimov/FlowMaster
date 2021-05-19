import datetime as dt
from typing import Iterator

from mock import Mock

from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.yandex_metrika_logs.export import (
    YandexMetrikaLogsExport,
)
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.operators.etl.types import DataOrient
from tests.fixtures.yandex_metrika import yml_visits_to_csv_config


def test_flow():
    def export_func(start_period, end_period) -> Iterator[tuple[dict, list, list]]:
        yield ExportContext(
            columns=["col1"], data=[[start_period]], data_orient=DataOrient.values
        )
        yield ExportContext(
            columns=["col1"], data=[[end_period]], data_orient=DataOrient.values
        )

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)

    yml_visits_to_csv_config.load.file_name = f"{test_flow.__name__}.tsv"
    yml_visits_to_csv_config.load.with_columns = True

    flow = ETLOperator(yml_visits_to_csv_config)

    list(flow(start_period=dt.datetime(2021, 1, 1), end_period=dt.datetime(2021, 1, 2)))
