import datetime as dt

from flowmaster.operators.etl.policy import ETLFlowConfig
from flowmaster.operators.etl.service import ETLOperator
from tests.fixtures.fakedata import fakedata_to_csv_config


def test_flow_fakedata():
    config = ETLFlowConfig(**dict(fakedata_to_csv_config))
    etl_flow = ETLOperator(config)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))