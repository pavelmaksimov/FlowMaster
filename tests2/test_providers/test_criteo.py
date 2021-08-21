import datetime as dt

from flowmaster.operators.etl.core import ETLOperator


def test(criteo_to_csv_notebook):
    flow = ETLOperator(criteo_to_csv_notebook)
    flow.dry_run(dt.datetime(2021, 7, 27), dt.datetime(2021, 7, 27))
    with flow.Load.open_file() as file:
        assert file.read() == 'Day\tClicks\n"2021-07-27"\t"1927"\n'
