import datetime as dt
import logging

import mock
import pendulum

from flowmaster.models import FlowItem, FlowStatus
from flowmaster.operators.base.work import Work, ordering_flow_tasks
from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.utils.yaml_helper import YamlHelper
from tests.fixtures.yandex_metrika import yml_visits_to_csv_notebook as NOTEBOOK

FLOW_NAME = "test_work"

logger = logging.getLogger(__name__)


def test_ordering_flow_tasks():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    NOTEBOOK.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone="Europe/Moscow",
            start_time="00:00:00",
            from_date=dt.date.today() - dt.timedelta(5),
            interval="daily",
        )
    )
    notebook = dict(NOTEBOOK)
    notebook.pop("name")

    rv = [(FLOW_NAME, notebook)]
    YamlHelper.iter_parse_file_from_dir = mock.Mock(return_value=rv)

    flows = list(ordering_flow_tasks())

    assert len(flows) == 5
    assert FlowItem.count_items(FLOW_NAME, statuses=[FlowStatus.run]) == len(flows)


def test_ordering_flow_tasks_with_period_length():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    NOTEBOOK.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone="Europe/Moscow",
            start_time="00:00:00",
            from_date=dt.date.today() - dt.timedelta(5),
            interval="daily",
            period_length=2,
        )
    )
    notebook = dict(NOTEBOOK)
    notebook.pop("name")

    rv = [(FLOW_NAME, notebook)]
    YamlHelper.iter_parse_file_from_dir = mock.Mock(return_value=rv)

    flows = list(ordering_flow_tasks())

    assert len(flows) == 3


def test_worktime():
    tz = "Europe/Moscow"
    NOTEBOOK.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone=tz, start_time="01:00:00", from_date=None, interval="daily"
        )
    )
    work = Work(NOTEBOOK)
    assert work.current_worktime == pendulum.yesterday(tz).replace(hour=1)

    NOTEBOOK.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone="Europe/Moscow",
            start_time="01:00:00",
            from_date=dt.date.today() - dt.timedelta(5),
            interval="daily",
        )
    )
    work = Work(NOTEBOOK)
    assert work.current_worktime == pendulum.yesterday(tz).replace(hour=1)


def test_worktime_second_interval():
    tz = "Europe/Moscow"
    NOTEBOOK.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone=tz, start_time="01:00:00", from_date=None, interval=86400
        )
    )
    work = Work(NOTEBOOK)
    assert work.current_worktime == pendulum.today(tz).replace(hour=1)

    NOTEBOOK.work.triggers.schedule = (
        ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone="Europe/Moscow",
            start_time="01:00:00",
            from_date=dt.date.today() - dt.timedelta(5),
            interval=86400,
        )
    )
    work = Work(NOTEBOOK)

    assert work.current_worktime == pendulum.today(tz).replace(hour=1)


def test_ordering_flow_tasks_with_interval_seconds(flowitem_model):
    now = pendulum.datetime(2021, 1, 1)
    pendulum.set_test_now(now)

    for _ in range(5):
        NOTEBOOK.work.triggers.schedule = (
            ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
                timezone="UTC", start_time="00:00:00", from_date=None, interval=60
            )
        )

        notebook = NOTEBOOK.dict()
        notebook.pop("name")
        rv = [(flowitem_model.name_for_test, notebook)]
        YamlHelper.iter_parse_file_from_dir = mock.Mock(return_value=rv)

        list(ordering_flow_tasks())

        now += dt.timedelta(seconds=60)
        pendulum.set_test_now(now)

    assert FlowItem.count_items(flowitem_model.name_for_test) == 5
