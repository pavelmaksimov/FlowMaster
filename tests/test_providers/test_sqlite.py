import datetime as dt

from flowmaster.operators.etl.service import ETLOperator


def test_flow_sqlite_to_csv(sqlite_to_csv_config):
    etl_flow = ETLOperator(sqlite_to_csv_config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))

    with etl_flow.Load.open_file(mode="r") as loadfile:
        data = loadfile.read()

    # fmt: off
    assert data == '''id\tkey
1\t"one"
2\t"two"
'''
