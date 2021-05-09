import datetime as dt
from typing import Iterator

from mock import Mock

from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseLoadPolicy
from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoad
from flowmaster.operators.etl.providers.yandex_metrika_logs.export import (
    YandexMetrikaLogsExport,
)
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.operators.etl.types import DataOrient
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir
from tests.fixtures.yandex_metrika import yml_visits_to_clickhouse_config as CONFIG

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")
CONFIG.load.credentials = ClickhouseLoadPolicy.Credentials(**credentials["clickhouse"])


def test_real_load_clickhouse():
    def export_func(start_period, end_period) -> Iterator[tuple[dict, list, list]]:
        yield ExportContext(columns=["date"], data=[[start_period]], data_orient=DataOrient.values)

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)

    CONFIG.load.table_schema.table = test_real_load_clickhouse.__name__

    flow = ETLOperator(CONFIG)
    flow.Load.Table.drop_table()
    try:
        list(
            flow(
                start_period=dt.datetime(2021, 1, 1), end_period=dt.datetime(2021, 1, 1)
            )
        )

        assert flow.Load.Table.select(columns=["Date"]) == [(dt.date(2021, 1, 1),)]

        # test data_cleaning_mode off

        CONFIG.load.data_cleaning_mode = ClickhouseLoad.DataCleaningMode.off
        flow = ETLOperator(CONFIG)

        list(
            flow(
                start_period=dt.datetime(2021, 1, 2), end_period=dt.datetime(2021, 1, 2)
            )
        )

        assert flow.Load.Table.select(columns=["Date"]) == [
            (dt.date(2021, 1, 1),),
            (dt.date(2021, 1, 2),),
        ]

        # test data_cleaning_mode partition

        CONFIG.load.data_cleaning_mode = ClickhouseLoad.DataCleaningMode.partition
        flow = ETLOperator(CONFIG)

        list(
            flow(
                start_period=dt.datetime(2021, 1, 2), end_period=dt.datetime(2021, 1, 2)
            )
        )

        assert flow.Load.Table.select(columns=["Date"]) == [
            (dt.date(2021, 1, 1),),
            (dt.date(2021, 1, 2),),
        ]

        # test data_cleaning_mode truncate

        CONFIG.load.data_cleaning_mode = ClickhouseLoad.DataCleaningMode.truncate
        flow = ETLOperator(CONFIG)

        list(
            flow(
                start_period=dt.datetime(2021, 1, 2), end_period=dt.datetime(2021, 1, 2)
            )
        )

        assert flow.Load.Table.select(columns=["Date"]) == [(dt.date(2021, 1, 2),)]

    finally:
        flow.Load.Table.drop_table()
