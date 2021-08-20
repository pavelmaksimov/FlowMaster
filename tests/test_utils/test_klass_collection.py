from flowmaster.flow import Flow


def test_flow_collection(fakedata_to_csv_notebook):
    flow = Flow(fakedata_to_csv_notebook)
    assert flow.name == Flow.ETLOperator.name

    flow = Flow(fakedata_to_csv_notebook.dict())
    assert flow.name == Flow.ETLOperator.name
