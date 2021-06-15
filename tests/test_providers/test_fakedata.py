import datetime as dt

from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.operators.etl.service import ETLOperator
from tests.fixtures.fakedata import fakedata_to_csv_notebook


def test_flow_fakedata():
    notebook = ETLNotebook(**dict(fakedata_to_csv_notebook))
    etl_flow = ETLOperator(notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))
