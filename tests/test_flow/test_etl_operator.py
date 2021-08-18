import datetime as dt
from typing import Iterator

from mock import Mock


def test_flow(ya_metrika_logs_to_csv_notebook):
    from flowmaster.operators.etl.dataschema import ExportContext
    from flowmaster.operators.etl.providers import Providers
    from flowmaster.operators.etl.core import ETLOperator
    from flowmaster.operators.etl.enums import DataOrient

    def export_func(
        start_period, end_period, **kwargs
    ) -> Iterator[tuple[dict, list, list]]:
        yield ExportContext(
            columns=["col1"], data=[[start_period]], data_orient=DataOrient.values
        )
        yield ExportContext(
            columns=["col1"], data=[[end_period]], data_orient=DataOrient.values
        )

    Providers.YandexMetrikaLogsProvider.export_class.__call__ = Mock(side_effect=export_func)

    ya_metrika_logs_to_csv_notebook.load.file_name = f"{test_flow.__name__}.tsv"
    ya_metrika_logs_to_csv_notebook.load.with_columns = True

    flow = ETLOperator(ya_metrika_logs_to_csv_notebook)
    flow.dry_run(
        start_period=dt.datetime(2021, 1, 1), end_period=dt.datetime(2021, 1, 2)
    )
