import datetime as dt

from flowmaster.operators.etl.core import ETLOperator


def test_flow_csv_to_csv_with_columns(csv_to_csv_with_columns_notebook):
    etl_flow = ETLOperator(csv_to_csv_with_columns_notebook)
    etl_flow.dry_run(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1))

    with etl_flow.Load.open_file(mode="r") as loadfile:
        data = loadfile.readlines()

    assert data == [
        "col1\tcol2\n",
        '"1"\tnull\n',
        '"1"\t"2"\n',
        '"1"\t"2"\n',
        '"1"\t"2"\n',
        '"1"\t"2"\n',
    ]


def test_flow_csv_to_csv_without_columns(csv_to_csv_without_columns_notebook):
    etl_flow = ETLOperator(csv_to_csv_without_columns_notebook)
    etl_flow.dry_run(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1))

    with etl_flow.Load.open_file(mode="r") as loadfile:
        data = loadfile.readlines()

    assert data == [
        "col1\tcol2\n",
        '"1"\tnull\n',
        '"1"\t"2"\n',
        '"1"\t"2"\n',
        '"1"\t"2"\n',
        '"1"\t"2"\n',
    ]
