import mock

from flowmaster.utils.local_executor import sync_executor


def test_local_executor(flowitem_model, fakedata_to_csv_notebook):
    with mock.patch(
        "flowmaster.service.iter_active_notebook_filenames"
    ) as a, mock.patch("flowmaster.service.get_notebook") as b:
        a.return_value = fakedata_to_csv_notebook.name
        b.return_value = (True, None, None, fakedata_to_csv_notebook, None)

        from flowmaster.operators.base.work import ordering_flow_tasks

        tasks = list(ordering_flow_tasks())

    sync_executor(orders=1, dry_run=True)

    assert len(tasks) == 5
