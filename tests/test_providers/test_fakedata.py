import datetime as dt


def test_flow_fakedata(fakedata_to_csv_notebook):
    from flowmaster.operators.etl.service import ETLOperator

    etl_flow = ETLOperator(fakedata_to_csv_notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))
