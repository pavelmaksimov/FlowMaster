import datetime as dt
from typing import Iterator

from mock import Mock

from flowmaster.flow import Flow
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.loaders.clickhouse.service import ClickhouseLoader


def test_real_load_clickhouse(csv_to_clickhouse_notebook):
    def export_func(start_period, end_period) -> Iterator[tuple[dict, list, list]]:
        yield ExportContext(
            columns=["date"], data=[[start_period]], data_orient=DataOrient.values
        )

    Flow.ETLOperator.Providers.CSVProvider.export_class.__call__ = Mock(
        side_effect=export_func
    )

    flow = Flow(csv_to_clickhouse_notebook)
    flow.Load.Table.drop_table()
    try:
        list(
            flow(
                start_period=dt.datetime(2021, 1, 1), end_period=dt.datetime(2021, 1, 1)
            )
        )

        assert flow.Load.Table.select(columns=["Date"]) == [(dt.date(2021, 1, 1),)]

        # test data_cleaning_mode off

        csv_to_clickhouse_notebook.load.data_cleaning_mode = (
            ClickhouseLoader.DataCleaningMode.off
        )
        flow = Flow(csv_to_clickhouse_notebook)

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

        csv_to_clickhouse_notebook.load.data_cleaning_mode = (
            ClickhouseLoader.DataCleaningMode.partition
        )
        flow = Flow(csv_to_clickhouse_notebook)

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

        csv_to_clickhouse_notebook.load.data_cleaning_mode = (
            ClickhouseLoader.DataCleaningMode.truncate
        )
        flow = Flow(csv_to_clickhouse_notebook)

        list(
            flow(
                start_period=dt.datetime(2021, 1, 2), end_period=dt.datetime(2021, 1, 2)
            )
        )

        assert flow.Load.Table.select(columns=["Date"]) == [(dt.date(2021, 1, 2),)]

    finally:
        flow.Load.Table.drop_table()
