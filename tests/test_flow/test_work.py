import datetime as dt
import logging

import mock
import pendulum

from flowmaster.models import FlowItem, FlowStatus
from flowmaster.operators.base.work import Work, order_flow
from flowmaster.operators.etl.policy import ETLFlowConfig
from flowmaster.utils.yaml_helper import YamlHelper
from tests.fixtures.yandex_metrika import yml_visits_to_file_config as CONFIG

FLOW_NAME = "test_work"

logger = logging.getLogger(__name__)


def test_order_flow():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    CONFIG.work.schedule = ETLFlowConfig.WorkPolicy.SchedulePolicy(
        timezone="Europe/Moscow",
        start_time="00:00:00",
        from_date=dt.date.today() - dt.timedelta(5),
        interval="daily",
    )
    config = dict(CONFIG)
    config.pop("name")

    rv = [(FLOW_NAME, config)]
    YamlHelper.iter_parse_file_from_dir = mock.Mock(return_value=rv)

    flows = list(order_flow(logger=logger))

    assert len(flows) == 5
    assert FlowItem.count_items(FLOW_NAME, statuses=[FlowStatus.run]) == len(flows)


def test_order_flow_with_period_length():
    FlowItem.delete().where(FlowItem.name == FLOW_NAME).execute()

    CONFIG.work.schedule = ETLFlowConfig.WorkPolicy.SchedulePolicy(
        timezone="Europe/Moscow",
        start_time="00:00:00",
        from_date=dt.date.today() - dt.timedelta(5),
        interval="daily",
        period_length=2,
    )
    config = dict(CONFIG)
    config.pop("name")

    rv = [(FLOW_NAME, config)]
    YamlHelper.iter_parse_file_from_dir = mock.Mock(return_value=rv)

    flows = list(order_flow(logger=logger))

    assert len(flows) == 3


def test_worktime():
    tz = "Europe/Moscow"
    CONFIG.work.schedule = ETLFlowConfig.WorkPolicy.SchedulePolicy(
        timezone=tz, start_time="01:00:00", from_date=None, interval="daily"
    )
    work = Work(CONFIG)
    assert work.current_worktime == pendulum.yesterday(tz).replace(hour=1)

    CONFIG.work.schedule = ETLFlowConfig.WorkPolicy.SchedulePolicy(
        timezone="Europe/Moscow",
        start_time="01:00:00",
        from_date=dt.date.today() - dt.timedelta(5),
        interval="daily",
    )
    work = Work(CONFIG)
    assert work.current_worktime == pendulum.yesterday(tz).replace(hour=1)


def test_worktime_second_interval():
    tz = "Europe/Moscow"
    CONFIG.work.schedule = ETLFlowConfig.WorkPolicy.SchedulePolicy(
        timezone=tz, start_time="01:00:00", from_date=None, interval=86400
    )
    work = Work(CONFIG)
    assert work.current_worktime == pendulum.today(tz).replace(hour=1)

    CONFIG.work.schedule = ETLFlowConfig.WorkPolicy.SchedulePolicy(
        timezone="Europe/Moscow",
        start_time="01:00:00",
        from_date=dt.date.today() - dt.timedelta(5),
        interval=86400,
    )
    work = Work(CONFIG)
    assert work.current_worktime == pendulum.today(tz).replace(hour=1)
