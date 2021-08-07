import datetime as dt


def test_flow_fakedata(fakedata_to_csv_notebook):
    from flowmaster.operators.etl.core import ETLOperator

    etl_flow = ETLOperator(fakedata_to_csv_notebook)
    etl_flow.dry_run(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1))
