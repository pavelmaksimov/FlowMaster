import datetime as dt

from flowmaster.operators.etl.core import ETLOperator


def test_flow_postgres_to_csv(postgres_to_csv_notebook):
    etl_flow = ETLOperator(postgres_to_csv_notebook)
    etl_flow.dry_run(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1))

    with etl_flow.Load.open_file(mode="r") as loadfile:
        data = loadfile.read()

    # fmt: off
    assert data == '''id\tkey
2\t"two"
1\t"one"
'''
