import datetime as dt
import logging

import mock
import pendulum

from flowmaster.models import FlowItem, FlowStatus
from flowmaster.operators.base.work import Work, ordering_flow_tasks
from flowmaster.operators.etl.policy import ETLNotebook

FLOW_NAME = "test_work"

logger = logging.getLogger(__name__)


def test_ordering_flow_tasks(flowitem_model, ya_metrika_logs_to_csv_notebook):
    name = flowitem_model.name_for_test

    with mock.patch(
        "flowmaster.service.iter_active_notebook_filenames"
    ) as a, mock.patch("flowmaster.service.get_notebook") as b:
        a.return_value = name
        b.return_value = (True, None, None, ya_metrika_logs_to_csv_notebook, None)

        tasks = list(ordering_flow_tasks())

        assert len(tasks) == 5
        assert flowitem_model.count_items(name, statuses=[FlowStatus.run]) == len(tasks)


def test_prepare_items_for_order(flowitem_model, flowmasterdata_items_to_csv_notebook):
    name = flowitem_model.name_for_test
    flowmasterdata_items_to_csv_notebook.name = name
    work = Work(flowmasterdata_items_to_csv_notebook)

    with mock.patch(
        "flowmaster.service.iter_active_notebook_filenames"
    ) as a, mock.patch("flowmaster.service.get_notebook") as b:
        a.return_value = name
        b.return_value = (True, None, None, flowmasterdata_items_to_csv_notebook, None)

        list(ordering_flow_tasks())

        for i in flowitem_model.iter_items(name, statuses=[FlowStatus.run]):
            assert pendulum.parse(i.expires_utc, tz="UTC") == work.expires


def test_ordering_flow_tasks_with_period_length(
    flowitem_model, ya_metrika_logs_to_csv_notebook
):
    name = flowitem_model.name_for_test
    ya_metrika_logs_to_csv_notebook.name = name
    ya_metrika_logs_to_csv_notebook.work.triggers.schedule.period_length = 2

    with mock.patch(
        "flowmaster.service.iter_active_notebook_filenames"
    ) as a, mock.patch("flowmaster.service.get_notebook") as b:
        a.return_value = name
        b.return_value = (True, None, None, ya_metrika_logs_to_csv_notebook, None)

        tasks = list(ordering_flow_tasks())

    assert len(tasks) == 3


def test_worktime(flowitem_model, ya_metrika_logs_to_csv_notebook):
    tz = "Europe/Moscow"
    ya_metrika_logs_to_csv_notebook.name = flowitem_model.name_for_test
    ya_metrika_logs_to_csv_notebook.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone=tz, start_time="01:00:00", from_date=None, interval="daily"
        )
    )
    work = Work(ya_metrika_logs_to_csv_notebook)

    assert work.current_worktime == pendulum.yesterday(tz).replace(hour=1)

    ya_metrika_logs_to_csv_notebook.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone="Europe/Moscow",
            start_time="01:00:00",
            from_date=dt.date.today() - dt.timedelta(5),
            interval="daily",
        )
    )
    work = Work(ya_metrika_logs_to_csv_notebook)

    assert work.current_worktime == pendulum.yesterday(tz).replace(hour=1)


def test_worktime_second_interval(flowitem_model, ya_metrika_logs_to_csv_notebook):
    tz = "Europe/Moscow"
    ya_metrika_logs_to_csv_notebook.name = flowitem_model.name_for_test
    ya_metrika_logs_to_csv_notebook.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone=tz, start_time="01:00:00", from_date=None, interval=86400
        )
    )
    work = Work(ya_metrika_logs_to_csv_notebook)

    assert work.current_worktime == pendulum.today(tz).replace(hour=1)

    ya_metrika_logs_to_csv_notebook.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone="Europe/Moscow",
            start_time="01:00:00",
            from_date=dt.date.today() - dt.timedelta(5),
            interval=86400,
        )
    )
    work = Work(ya_metrika_logs_to_csv_notebook)

    assert work.current_worktime == pendulum.today(tz).replace(hour=1)


def test_ordering_flow_tasks_with_interval_seconds(
    flowitem_model, ya_metrika_logs_to_csv_notebook
):
    now = pendulum.datetime(2021, 1, 1)
    pendulum.set_test_now(now)
    name = flowitem_model.name_for_test
    ya_metrika_logs_to_csv_notebook.name = name
    ya_metrika_logs_to_csv_notebook.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone="UTC", start_time="00:00:00", from_date=None, interval=60
        )
    )

    for _ in range(5):
        with mock.patch(
            "flowmaster.service.iter_active_notebook_filenames"
        ) as a, mock.patch("flowmaster.service.get_notebook") as b:
            a.return_value = name
            b.return_value = (True, None, None, ya_metrika_logs_to_csv_notebook, None)

            list(ordering_flow_tasks())

            now += dt.timedelta(seconds=60)
            pendulum.set_test_now(now)

    assert FlowItem.count_items(name) == 5
