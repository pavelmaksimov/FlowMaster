import datetime as dt
import functools
import time
from logging import getLogger

from mock import Mock

from flowmaster.operators.etl.providers.yandex_metrika_logs.export import (
    YandexMetrikaLogsExport,
)
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.pool import pools
from flowmaster.utils.executor import catch_exceptions, SleepTask, Executor
from tests.fixtures.yandex_metrika import yml_visits_to_file_config

logger_ = getLogger(__name__)
logger_.level = 20

Executor = functools.partial(Executor, dry_run=True)


class TestSleepFunc:
    def test_sleep_func(self):
        """
        Checking that 1 worker executes in 6 seconds,
        processes with a total duration of 15 seconds.
        """
        self.count = 0
        self.results = []

        def func():
            for i in range(5):
                yield SleepTask(sleep=1)
                self.results.append(i)

        @catch_exceptions
        def order_task(*args, **kwargs):
            if self.count == 0:
                for i in range(3):
                    yield func()
            self.count += 1

        flow_scheduler = Executor(order_task_func=order_task)
        flow_scheduler.start(workers=1, order_interval=0.5, orders=1)

        time.sleep(6)
        assert len(self.results) == 15


def test_sleep():
    s = SleepTask(sleep=1)
    time.sleep(0)
    assert s.allow() == False
    time.sleep(1.1)
    assert s.allow() == True


def test_executor():
    def export_func(start_date, end_date):
        yield ({}, ["col1"], [[start_date]])

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)

    @catch_exceptions
    def order_task(*args, **kwargs):
        count_flows = 4
        worktimes = [dt.datetime(2021, 1, i + 1) for i in range(count_flows)]

        for worktime in worktimes:
            yml_visits_to_file_config.load.file_name = f"{test_executor.__name__}.tsv"

            flow = ETLOperator(yml_visits_to_file_config)
            generator = flow(start_period=worktime, end_period=worktime)

            yield generator

    flow_scheduler = Executor(order_task_func=order_task)
    flow_scheduler.start(workers=1, order_interval=3, orders=2)

    assert True  # duration >= count_flows * duration_func / size_pools


def test_executor_concurrency():
    duration_func = 5

    def export_func(start_date, end_date):
        time.sleep(duration_func)
        yield ({}, ["col1"], [[start_date]])

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)

    @catch_exceptions
    def order_task(*args, **kwargs):
        count_flows = 4
        worktimes = [dt.datetime(2021, 1, i + 1) for i in range(count_flows)]

        for worktime in worktimes:
            yml_visits_to_file_config.load.file_name = (
                f"{test_executor_concurrency.__name__}.tsv"
            )
            yml_visits_to_file_config.work.concurrency = 1
            yml_visits_to_file_config.export.concurrency = 4
            yml_visits_to_file_config.transform.concurrency = 4
            yml_visits_to_file_config.load.concurrency = 4

            flow = ETLOperator(yml_visits_to_file_config)
            generator = flow(start_period=worktime, end_period=worktime)

            yield generator

    flow_scheduler = Executor(order_task_func=order_task)
    flow_scheduler.start(workers=4, order_interval=1, orders=1)

    assert True  # duration >= count_flows * duration_func / concurrency


def test_executor_pools():
    duration_func = 5

    def export_func(start_date, end_date):
        logger_.info("wait export")
        time.sleep(duration_func)
        yield ({}, ["col1"], [[start_date]])

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)

    @catch_exceptions
    def order_task(*args, **kwargs):
        count_flows = 4
        worktimes = [dt.datetime(2021, 1, i + 1) for i in range(count_flows)]

        for worktime in worktimes:
            pools.append_pools({"two": 2})
            yml_visits_to_file_config.load.file_name = (
                f"{test_executor_pools.__name__}.tsv"
            )
            yml_visits_to_file_config.export.pools = ["two"]

            flow = ETLOperator(yml_visits_to_file_config)
            generator = flow(start_period=worktime, end_period=worktime)

            yield generator

    flow_scheduler = Executor(order_task_func=order_task)
    flow_scheduler.start(workers=4, order_interval=1, orders=1)

    assert True  # duration >= count_flows * duration_func / size_pools
