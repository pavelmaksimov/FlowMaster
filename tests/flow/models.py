import datetime as dt

import pendulum
import pytest
from mock import Mock

from flowmaster.models import FlowItem, FlowStatus
from flowmaster.utils import iter_range_datetime

FLOW_NAME = "test_schedule"


@pytest.mark.parametrize(
    "create_retries,retries,result", [(0, 0, 0), (0, 1, 1), (1, 1, 0), (1, 0, 0)]
)
def test_retries(create_retries, retries, result):
    # TODO: Не работает с started_utc=None
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    FlowItem.create(
        **{
            FlowItem.name.name: FLOW_NAME,
            FlowItem.worktime.name: pendulum.datetime(
                2020, 1, 1, tz="Europe/Moscow"
            ),
            FlowItem.started_utc.name: dt.datetime(2020, 1, 1),
            FlowItem.status.name: FlowStatus.error,
            FlowItem.retries.name: create_retries,
        }
    )

    FlowItem.retry_error_items(FLOW_NAME, retries=retries, retry_delay=60)

    items = FlowItem.select().where(
        FlowItem.name == FLOW_NAME, FlowItem.status == FlowStatus.add
    )

    assert len(items) == int(result)


@pytest.mark.parametrize(
    "retry_delay,passed_sec,is_run", [(10, 9, False), (10, 10, True), (10, 11, True)]
)
def test_retry_delay(retry_delay, passed_sec, is_run):
    # TODO: Не работает с started_utc=None
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    FlowItem.create(
        **{
            FlowItem.name.name: FLOW_NAME,
            FlowItem.worktime.name: pendulum.datetime(
                2020, 1, 1, tz="Europe/Moscow"
            ),
            FlowItem.started_utc.name: dt.datetime(2020, 1, 1, 0, 0, 0),
            FlowItem.status.name: FlowStatus.error,
            FlowItem.retries.name: 0,
        }
    )

    FlowItem.get_utcnow = Mock(return_value=dt.datetime(2020, 1, 1, 0, 0, passed_sec))

    FlowItem.retry_error_items(FLOW_NAME, retries=1, retry_delay=retry_delay)

    items = FlowItem.select().where(
        FlowItem.name == FLOW_NAME, FlowItem.status == FlowStatus.add
    )

    assert len(items) == int(is_run)


def test_create_next_execute_item():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)

    item = FlowItem.create_next_execute_item(
        flow_name=FLOW_NAME,
        worktime=worktime,
        interval_timedelta=interval_timedelta,
    )

    assert item is None

    FlowItem.create(
        **{
            FlowItem.name.name: FLOW_NAME,
            FlowItem.worktime.name: worktime - dt.timedelta(1),
        }
    )
    item = FlowItem.create_next_execute_item(
        flow_name=FLOW_NAME,
        worktime=worktime,
        interval_timedelta=interval_timedelta,
    )

    assert item

    item = FlowItem.create_next_execute_item(
        flow_name=FLOW_NAME,
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
                FlowItem.status.name: FlowStatus.success,
            }
        )

    items = FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 2
    assert FlowItem.count_items(FLOW_NAME, statuses=[FlowStatus.add]) == 2
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
                FlowItem.status.name: FlowStatus.error,
            }
        )

    items = FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 2
    assert FlowItem.count_items(FLOW_NAME, statuses=[FlowStatus.add]) == 2
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
            FlowItem.status.name: FlowStatus.error,
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
            FlowItem.status.name: FlowStatus.error,
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

    FlowItem.create(
        **{FlowItem.name.name: FLOW_NAME, FlowItem.worktime.name: worktime}
    )

    FlowItem.create_missing_items(
        flow_name=FLOW_NAME,
        start_time=worktime - dt.timedelta(5),
        end_time=worktime,
        interval_timedelta=interval_timedelta,
    )

    assert (
        FlowItem.select()
        .where(FlowItem.name == FLOW_NAME, FlowItem.status == FlowStatus.add)
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
        new_status=FlowStatus.success,
        from_time=worktime - dt.timedelta(5),
        to_time=worktime,
    )

    assert FlowItem.count_items(FLOW_NAME, statuses=[FlowStatus.success]) == len(items)


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
        status=FlowStatus.fatal_error,
        config_hash="",
    )
    assert (
        FlowItem.allow_execute_flow(FLOW_NAME, config_hash="", max_fatal_errors=3)
        is False
    )
    assert (
        FlowItem.allow_execute_flow(FLOW_NAME, config_hash="new", max_fatal_errors=3)
        is True
    )

    FlowItem.recreate_prev_items(
        flow_name=FLOW_NAME,
        worktime=worktime,
        offset_periods=[-1],
        interval_timedelta=interval_timedelta
    )
    assert (
        FlowItem.allow_execute_flow(FLOW_NAME, config_hash="", max_fatal_errors=3)
        is True
    )
