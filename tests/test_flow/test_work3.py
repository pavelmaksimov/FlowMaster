from unittest import mock


def test_ordering_flow_tasks_with_period_length(ya_metrika_logs_to_csv_notebook):
    ya_metrika_logs_to_csv_notebook.work.triggers.schedule.period_length = 2

    with mock.patch(
        "flowmaster.service.iter_active_notebook_filenames"
    ) as a, mock.patch("flowmaster.service.get_notebook") as b:
        a.return_value = ya_metrika_logs_to_csv_notebook.name
        b.return_value = (True, None, None, ya_metrika_logs_to_csv_notebook, None)

        from flowmaster.operators.base.work import ordering_flow_tasks

        tasks = list(ordering_flow_tasks())

    assert len(tasks) == 3
