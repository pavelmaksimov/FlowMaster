import datetime as dt
from typing import Iterator

from mock import Mock
from tests.fixtures.yandex_metrika import yml_visits_to_clickhouse_notebook as NOTEBOOK

from flowmaster.operators.etl.core import ETLOperator
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.loaders.clickhouse.policy import ClickhouseLoadPolicy
from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader
from flowmaster.operators.etl.providers.yandex_metrika_logs.export import (
    YandexMetrikaLogsExport,
)
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")
NOTEBOOK.load.credentials = ClickhouseLoadPolicy.CredentialsPolicy(
    **credentials["clickhouse"]
)


def test_real_load_clickhouse():
    def export_func(start_period, end_period) -> Iterator[tuple[dict, list, list]]:
        yield ExportContext(
            columns=["date"], data=[[start_period]], data_orient=DataOrient.values
        )

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)

    NOTEBOOK.load.table_schema.table = test_real_load_clickhouse.__name__

    flow = ETLOperator(NOTEBOOK)
    flow.Load.Table.drop_table()
    try:
        list(
            flow(
                start_period=dt.datetime(2021, 1, 1), end_period=dt.datetime(2021, 1, 1)
            )
        )

        assert flow.Load.Table.select(columns=["Date"]) == [(dt.date(2021, 1, 1),)]

        # test data_cleaning_mode off

        NOTEBOOK.load.data_cleaning_mode = ClickhouseLoader.DataCleaningMode.off
        flow = ETLOperator(NOTEBOOK)

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

        NOTEBOOK.load.data_cleaning_mode = ClickhouseLoader.DataCleaningMode.partition
        flow = ETLOperator(NOTEBOOK)

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

        NOTEBOOK.load.data_cleaning_mode = ClickhouseLoader.DataCleaningMode.truncate
        flow = ETLOperator(NOTEBOOK)

        list(
            flow(
                start_period=dt.datetime(2021, 1, 2), end_period=dt.datetime(2021, 1, 2)
            )
        )

        assert flow.Load.Table.select(columns=["Date"]) == [(dt.date(2021, 1, 2),)]

    finally:
        flow.Load.Table.drop_table()
