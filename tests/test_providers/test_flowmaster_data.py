import datetime as dt

from flowmaster.models import FlowItem
from flowmaster.operators.etl.service import ETLOperator


def test_flow_flowmaster_items(flowmasterdata_items_to_csv_notebook):
    FlowItem.clear(flowmasterdata_items_to_csv_notebook.name)

    etl_flow = ETLOperator(flowmasterdata_items_to_csv_notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 5)))

    with etl_flow.Load.open_file(mode="r") as loadfile:
        data = loadfile.readlines()

    count_items = len(
        [row for row in data if flowmasterdata_items_to_csv_notebook.name in row]
    )

    assert count_items == 5


def test_flow_flowmasterdata_pools(
    flowmasterdata_items_to_csv_notebook, flowmasterdata_pools_export_policy
):
    flowmasterdata_items_to_csv_notebook.export = flowmasterdata_pools_export_policy

    etl_flow = ETLOperator(flowmasterdata_items_to_csv_notebook)
    list(etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1)))

    with etl_flow.Load.open_file(mode="r") as loadfile:
        data = loadfile.readlines()

    assert [
        row
        for row in data
        if "__flowmasterdata_items_to_csv_export_concurrency__" in row
    ]
    assert [
        row
        for row in data
        if "__flowmasterdata_items_to_csv_transform_concurrency__" in row
    ]
    assert [
        row for row in data if "__flowmasterdata_items_to_csv_load_concurrency__" in row
    ]
    assert [row for row in data if "name\tsize\tlimit\tdatetime" in row]
