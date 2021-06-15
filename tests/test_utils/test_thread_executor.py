import datetime as dt
import functools
import time
from logging import getLogger
from typing import Iterator

from mock import Mock

from flowmaster.executors import (
    SleepIteration,
    ExecutorIterationTask,
    ThreadAsyncExecutor,
    NextIterationInPools,
)
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.yandex_metrika_logs.export import (
    YandexMetrikaLogsExport,
)
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.operators.etl.types import DataOrient
from flowmaster.pool import pools
from tests.fixtures.yandex_metrika import yml_visits_to_csv_notebook

logger_ = getLogger(__name__)
logger_.level = 20

ThreadExecutor = functools.partial(ThreadAsyncExecutor, dry_run=True)


def test_sanity_executor_sleep_task():
    """
    Checking that 1 worker executes in 3 seconds,
    processes with a total duration of 9 seconds.
    """
    completed_tasks = []
    count_flows = 3

    def flow():
        yield SleepIteration(sleep=2)
        completed_tasks.append(1)

    def order_flow_task(*args, **kwargs):
        for _ in range(count_flows):
            yield flow()

    executor = ThreadAsyncExecutor(ordering_task_func=order_flow_task, dry_run=True)
    executor.start(workers=1, orders=1)
    time.sleep(count_flows)
    executor.stop()

    assert len(completed_tasks) == count_flows


def test_sleep():
    s = SleepIteration(sleep=1)
    time.sleep(0)
    assert s.allow() == False
    time.sleep(1.1)
    assert s.allow() == True


def test_sanity_executor_flow():
    completed_tasks = []
    count_task = 4
    orders = 2

    def flow():
        yield
        completed_tasks.append(1)

    def ordering_task(*args, **kwargs):
        for _ in range(count_task):
            yield ExecutorIterationTask(flow())

    executor = ThreadAsyncExecutor(ordering_task_func=ordering_task, dry_run=True)
    executor.start(workers=4, orders=orders)
    time.sleep(1)
    executor.stop()

    assert len(completed_tasks) == count_task * orders


def test_pool_sleep_iterations():
    completed_tasks = []
    workers = 2
    count_task = 2
    orders = 2
    sleep = 0
    iters = 2
    pools.append_pools({"test_pool_sleep_iterations": 1})

    def flow():
        for _ in range(iters):
            yield NextIterationInPools(
                pool_names=["test_pool_sleep_iterations"], sleep=0
            )
            yield SleepIteration(sleep=sleep)
        completed_tasks.append(1)

    def ordering_task(*args, **kwargs):
        for _ in range(count_task):
            yield ExecutorIterationTask(flow())

    executor = ThreadAsyncExecutor(ordering_task_func=ordering_task, dry_run=False)
    executor.start(workers=workers, orders=orders)
    time.sleep(sleep * iters * orders * count_task + 10)
    executor.stop()

    assert len(completed_tasks) == count_task * orders


def test_concurrency():
    duration_func = 2
    count_task = 2
    concurrency = 1
    begin = time.time()
    completed_tasks = []

    def flow(start_date, end_date):
        time.sleep(duration_func)
        duration_from_begin = int(time.time() - begin)
        completed_tasks.append(duration_from_begin)

        yield ExportContext(
            columns=["col1"], data=[[start_date]], data_orient=DataOrient.values
        )

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=flow)

    def order_task(*args, **kwargs) -> Iterator[ExecutorIterationTask]:
        worktimes = [dt.datetime(2021, 1, i + 1) for i in range(count_task)]

        for worktime in worktimes:
            yml_visits_to_csv_notebook.load.file_name = (
                f"{test_concurrency.__name__}.tsv"
            )
            yml_visits_to_csv_notebook.work.concurrency = concurrency
            yml_visits_to_csv_notebook.export.concurrency = 4
            yml_visits_to_csv_notebook.transform.concurrency = 4
            yml_visits_to_csv_notebook.load.concurrency = 4

            flow = ETLOperator(yml_visits_to_csv_notebook)
            yield flow(start_period=worktime, end_period=worktime)

    executor = ThreadAsyncExecutor(ordering_task_func=order_task, dry_run=True)
    executor.start(workers=count_task, orders=1)
    time.sleep((count_task * duration_func / concurrency) + 1)
    if len(set(completed_tasks)) != count_task / concurrency:
        time.sleep(3)
    executor.stop()

    assert len(set(completed_tasks)) == count_task / concurrency
    assert sorted(completed_tasks)[-1] >= count_task * duration_func / concurrency


def test_pools():
    duration_func = 2
    count_task = 2
    pool_size = 2
    begin = time.time()
    completed_tasks = []

    def export_func(start_date, end_date):
        time.sleep(duration_func)
        duration_from_begin = int(time.time() - begin)
        completed_tasks.append(duration_from_begin)

        yield ExportContext(
            columns=["col1"], data=[[start_date]], data_orient=DataOrient.values
        )

    YandexMetrikaLogsExport.__call__ = Mock(side_effect=export_func)

    def order_task(*args, **kwargs) -> Iterator[ExecutorIterationTask]:
        worktimes = [dt.datetime(2021, 1, i + 1) for i in range(count_task)]
        pools.append_pools({"two": pool_size})

        for worktime in worktimes:
            yml_visits_to_csv_notebook.load.file_name = f"{test_pools.__name__}.tsv"
            yml_visits_to_csv_notebook.export.pools = ["two"]

            flow = ETLOperator(yml_visits_to_csv_notebook)
            task = flow(start_period=worktime, end_period=worktime)

            yield task

    executor = ThreadAsyncExecutor(ordering_task_func=order_task)
    executor.start(workers=count_task, interval=1, orders=1)
    time.sleep((count_task * duration_func / pool_size) + 5)
    executor.stop()

    assert len(set(completed_tasks)) == count_task / pool_size
    assert sorted(completed_tasks)[-1] >= count_task * duration_func / pool_size
