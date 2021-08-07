import datetime as dt
from unittest import mock

import pendulum


def test_ordering_flow_tasks_with_interval_seconds(
    pendulum_utcnow, flowitem_model, flowmasterdata_items_to_csv_notebook
):
    with mock.patch(
        "flowmaster.service.iter_active_notebook_filenames"
    ) as a100, mock.patch("flowmaster.service.get_notebook") as b100:
        a100.return_value = flowmasterdata_items_to_csv_notebook.name
        b100.return_value = (
            True,
            None,
            None,
            flowmasterdata_items_to_csv_notebook,
            None,
        )

        from flowmaster.operators.base.work import ordering_flow_tasks

        all_tasks = []
        for _ in range(4):
            tasks = list(ordering_flow_tasks())
            all_tasks += tasks
            assert len(tasks) == 1

            pendulum_utcnow += dt.timedelta(seconds=60)
            pendulum.set_test_now(pendulum_utcnow)

    assert len(all_tasks) == 4
