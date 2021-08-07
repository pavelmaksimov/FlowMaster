import datetime as dt
from unittest import mock

import pendulum


def test_ordering_flow_tasks(flowitem_model, ya_metrika_logs_to_csv_notebook):
    from flowmaster.enums import Statuses

    with mock.patch(
        "flowmaster.service.iter_active_notebook_filenames"
    ) as a, mock.patch("flowmaster.service.get_notebook") as b:
        a.return_value = ya_metrika_logs_to_csv_notebook.name
        b.return_value = (True, None, None, ya_metrika_logs_to_csv_notebook, None)

        from flowmaster.operators.base.work import ordering_flow_tasks

        tasks = list(ordering_flow_tasks())

        assert len(tasks) == 5
        assert (
            flowitem_model.count_items(
                ya_metrika_logs_to_csv_notebook.name, statuses=[Statuses.run]
            )
            == 5
        )


def test_expires_items(flowitem_model, flowmasterdata_items_to_csv_notebook):
    from flowmaster.enums import Statuses

    with mock.patch(
        "flowmaster.service.iter_active_notebook_filenames"
    ) as a, mock.patch("flowmaster.service.get_notebook") as b:
        a.return_value = flowmasterdata_items_to_csv_notebook.name
        b.return_value = (True, None, None, flowmasterdata_items_to_csv_notebook, None)

        from flowmaster.operators.base.work import Work, ordering_flow_tasks

        list(ordering_flow_tasks())

        work = Work(flowmasterdata_items_to_csv_notebook)
        for i in flowitem_model.iter_items(
            flowmasterdata_items_to_csv_notebook.name, statuses=[Statuses.run]
        ):
            assert pendulum.parse(i.expires_utc, tz="UTC") == work.expires
            assert i.name == flowmasterdata_items_to_csv_notebook.name


def test_current_worktime_daily(ya_metrika_logs_to_csv_notebook):
    from flowmaster.operators.etl.policy import ETLNotebook
    from flowmaster.operators.base.work import Work

    tz = "Europe/Moscow"
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
            from_date=pendulum.today("UTC") - dt.timedelta(5),
            interval="daily",
        )
    )
    work = Work(ya_metrika_logs_to_csv_notebook)

    assert work.current_worktime == pendulum.yesterday(tz).replace(hour=1)


def test_current_worktime_second_interval(
    flowitem_model, ya_metrika_logs_to_csv_notebook
):
    from flowmaster.operators.etl.policy import ETLNotebook
    from flowmaster.operators.base.work import Work

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
            from_date=pendulum.today(tz) - dt.timedelta(5),
            interval=86400,
        )
    )
    work = Work(ya_metrika_logs_to_csv_notebook)

    assert work.current_worktime == pendulum.today(tz).replace(hour=1)
