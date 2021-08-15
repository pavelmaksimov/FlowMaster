import datetime as dt

from flowmaster.operators.etl.core import ETLOperator


def test(google_sheets_to_csv_notebook):
    flow = ETLOperator(google_sheets_to_csv_notebook)
    list(flow(dt.datetime(2021, 7, 27), dt.datetime(2021, 7, 27)))
    with flow.Load.open_file() as file:
        text = file.read()
        assert text == (
            'col2\tdate\tcol1\n'
            '"2"\t"2021-01-01"\t"1"\n'
            '"2"\t"2021-01-01"\tnull\n'
            '"2"\tnull\tnull\n'
        )
