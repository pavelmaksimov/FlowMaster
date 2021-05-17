import datetime as dt

from flowmaster.operators.etl.service import ETLOperator


def test_flow_csv_to_csv_with_columns(config_csv_to_csv_with_columns):
    etl_flow = ETLOperator(config_csv_to_csv_with_columns)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))

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


def test_flow_csv_to_csv_without_columns(config_csv_to_csv_without_columns):
    etl_flow = ETLOperator(config_csv_to_csv_without_columns)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))

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
