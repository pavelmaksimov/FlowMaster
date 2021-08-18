import datetime as dt

import pendulum
import pytest

from flowmaster.enums import Statuses
from flowmaster.models import FlowItem
from flowmaster.utils import iter_range_datetime

FLOW_NAME = "test_schedule"


def test_datetime_utc_field(flowitem):
    flowitem.started_utc = pendulum.now("Europe/Moscow")
    flowitem.save()

    assert flowitem.started_utc == pendulum.now("UTC")


def test_filter_by_datetime_utc_field(pendulum_utctoday, flowitem, flowitem_model):
    flowitem.started_utc = pendulum.now("Europe/Minsk")
    flowitem.save()
    query = flowitem_model.select().where(flowitem_model.name == flowitem.name)

    assert query.where(
        flowitem_model.started_utc == pendulum.now("Europe/Moscow")
    ).first()
    assert query.where(flowitem_model.started_utc == pendulum.now("UTC")).first()


@pytest.mark.parametrize(
    "create_retries,retries,result", [(0, 0, 0), (0, 1, 1), (1, 1, 0), (1, 0, 0)]
)
def test_retries(create_retries, retries, result, pendulum_utctoday, flowitem_model):
    name = "__test_retries__"
    flowitem_model.clear(name)
    flowitem_model.create(
        **{
            FlowItem.name.name: name,
            FlowItem.worktime.name: pendulum_utctoday,
            FlowItem.finished_utc.name: pendulum_utctoday,
            FlowItem.status.name: Statuses.error,
            FlowItem.retries.name: create_retries,
        }
    )
    items = FlowItem.retry_error_items(name, retries=retries, retry_delay=0)

    assert len(items) == int(result)


@pytest.mark.parametrize("retry_delay,passed_sec", [(10, 5), (10, 10), (10, 11)])
def test_retry_delay(retry_delay, passed_sec, pendulum_utctoday, flowitem_model):
    name = "__test_retry_delay__"
    flowitem_model.clear(name)
    flowitem_model.create(
        **{
            FlowItem.name.name: name,
            FlowItem.worktime.name: pendulum_utctoday,
            FlowItem.finished_utc.name: pendulum_utctoday,
            FlowItem.status.name: Statuses.error,
            FlowItem.retries.name: 0,
        }
    )
    pendulum.set_test_now(pendulum_utctoday.add(seconds=passed_sec))
    items = list(
        flowitem_model.retry_error_items(name, retries=1, retry_delay=retry_delay)
    )

    assert len(items) == int(passed_sec >= retry_delay)


def test_create_next_execute_item(flowitem_model):
    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    item = FlowItem.create_next_execute_item(
        flow_name=flowitem_model.name_for_test,
        worktime=worktime,
        interval_timedelta=interval_timedelta,
    )

    assert item is None

    FlowItem.create(
        **{
            FlowItem.name.name: flowitem_model.name_for_test,
            FlowItem.worktime.name: worktime - dt.timedelta(1),
        }
    )
    item = FlowItem.create_next_execute_item(
        flow_name=flowitem_model.name_for_test,
        worktime=worktime,
        interval_timedelta=interval_timedelta,
    )

    assert item

    item = FlowItem.create_next_execute_item(
        flow_name=flowitem_model.name_for_test,
        worktime=worktime,
        interval_timedelta=interval_timedelta,
    )
    assert item is None


def test_create_update_items():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    items = FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert items is None

    for i in range(10):
        FlowItem.create(
            **{
                FlowItem.name.name: FLOW_NAME,
                FlowItem.worktime.name: worktime - dt.timedelta(i),
                FlowItem.status.name: Statuses.success,
            }
        )

    items = FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 2
    assert FlowItem.count_items(FLOW_NAME, statuses=[Statuses.add]) == 2
    for i in items:
        assert i.retries == 0


def test_create_update_error_items():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    items = FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert items is None

    for i in range(10):
        FlowItem.create(
            **{
                FlowItem.name.name: FLOW_NAME,
                FlowItem.worktime.name: worktime - dt.timedelta(i),
                FlowItem.status.name: Statuses.error,
            }
        )

    items = FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 2
    assert FlowItem.count_items(FLOW_NAME, statuses=[Statuses.add]) == 2
    for i in items:
        assert i.retries == 0


def test_create_update_items_before_start_time():
    """Checking when the update date is less than the first worktime."""
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    FlowItem.create(
        **{
            FlowItem.name.name: FLOW_NAME,
            FlowItem.worktime.name: worktime - dt.timedelta(1),
            FlowItem.status.name: Statuses.error,
        }
    )
    items = FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=3,
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 1


def test_create_update_items_start_time_equals_worktime():
    """Checking when the update date is equals the first worktime."""
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    FlowItem.create(
        **{
            FlowItem.name.name: FLOW_NAME,
            FlowItem.worktime.name: worktime,
            FlowItem.status.name: Statuses.error,
        }
    )
    items = FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=[-1, -2, -3],
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 0


def test_create_history_items():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    item = FlowItem.create_missing_items(
        flow_name=FLOW_NAME,
        start_time=worktime - dt.timedelta(5),
        end_time=worktime,
        interval_timedelta=interval_timedelta,
    )

    assert len(item) == 6


def test_create_missing_items():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 6, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    items = FlowItem.create_missing_items(
        flow_name=FLOW_NAME,
        start_time=worktime - dt.timedelta(5),
        end_time=worktime - dt.timedelta(5),
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 1

    FlowItem.create(**{FlowItem.name.name: FLOW_NAME, FlowItem.worktime.name: worktime})

    FlowItem.create_missing_items(
        flow_name=FLOW_NAME,
        start_time=worktime - dt.timedelta(5),
        end_time=worktime,
        interval_timedelta=interval_timedelta,
    )

    assert (
        FlowItem.select()
        .where(FlowItem.name == FLOW_NAME, FlowItem.status == Statuses.add)
        .count()
    ) == 6


def test_change_status():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 6, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    items = FlowItem.create_missing_items(
        flow_name=FLOW_NAME,
        start_time=worktime - dt.timedelta(5),
        end_time=worktime,
        interval_timedelta=interval_timedelta,
    )

    FlowItem.change_status(
        FLOW_NAME,
        new_status=Statuses.success,
        from_time=worktime - dt.timedelta(5),
        to_time=worktime,
    )

    assert FlowItem.count_items(FLOW_NAME, statuses=[Statuses.success]) == len(items)


def test_allow_execute_flow():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 6, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)
    worktime_list = iter_range_datetime(
        start_time=worktime - dt.timedelta(3),
        end_time=worktime,
        timedelta=interval_timedelta,
    )

    FlowItem.create_items(
        flow_name=FLOW_NAME,
        worktime_list=worktime_list,
        status=Statuses.fatal_error,
        notebook_hash="",
    )
    assert (
        FlowItem.allow_execute_flow(FLOW_NAME, notebook_hash="", max_fatal_errors=3)
        is False
    )
    assert (
        FlowItem.allow_execute_flow(FLOW_NAME, notebook_hash="new", max_fatal_errors=3)
        is True
    )

    FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=10,
        interval_timedelta=interval_timedelta,
    )
    assert (
        FlowItem.allow_execute_flow(FLOW_NAME, notebook_hash="", max_fatal_errors=3)
        is True
    )


def test_items_for_execute_seconds_interval_without_keep_sequence(flowitem_model):
    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")

    FlowItem.create_items(
        flowitem_model.name_for_test,
        worktime_list=[worktime - dt.timedelta(minutes=4)],
        **{flowitem_model.status.name: Statuses.success}
    )

    items = FlowItem.get_items_for_execute(
        flow_name=flowitem_model.name_for_test,
        worktime=worktime,
        start_time=worktime - dt.timedelta(minutes=10),
        interval_timedelta=dt.timedelta(minutes=1),
        keep_sequence=False,
        retries=0,
        retry_delay=0,
        notebook_hash="",
        max_fatal_errors=3,
    )

    assert len(items) == 1


def test_items_for_execute_seconds_interval_with_keep_sequence(flowitem_model):
    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")

    items = FlowItem.get_items_for_execute(
        flow_name=flowitem_model.name_for_test,
        worktime=worktime,
        start_time=worktime - dt.timedelta(minutes=9),
        interval_timedelta=dt.timedelta(minutes=1),
        keep_sequence=True,
        retries=2,
        retry_delay=0,
        notebook_hash="",
        max_fatal_errors=1,
    )

    assert len(items) == 10
