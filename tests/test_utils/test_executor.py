import time

import pendulum
import pytest

from flowmaster.executors import (
    SleepIteration,
    NextIterationInPools,
    ExpiredError,
    SoftTimeLimitError,
    SleepException,
    PoolOverflowingException,
    ExecutorIterationTask,
)


def test_expires():
    generator = (time.sleep(1) for i in range(1))
    task = ExecutorIterationTask(generator, expires=pendulum.now())

    with pytest.raises(ExpiredError):
        list(task)


def test_soft_time_limit():
    generator = (time.sleep(2) for i in range(1))
    task = ExecutorIterationTask(generator, soft_time_limit_seconds=1)

    with pytest.raises(SoftTimeLimitError):
        list(task)


def test_sleep_task():
    generator = iter([SleepIteration(sleep=1)])
    task = ExecutorIterationTask(generator)

    with pytest.raises(SleepException):
        try:
            list(task)
        except SleepException as sleep_task:
            assert sleep_task.sleep == 1
            raise


def test_exeption_iteration_in_pool(pools):
    pools.append_pools({"testpool": 0})
    generator = iter([NextIterationInPools(pool_names=["testpool"]), "item"])
    task = ExecutorIterationTask(generator)

    with pytest.raises(PoolOverflowingException):
        try:
            list(task)
        except PoolOverflowingException as sleep_task:
            assert sleep_task.sleep == 1
            raise


def test_exeption_iteration_in_pool2(pools):
    pools.append_pools({"testpool": 0})
    generator = iter([NextIterationInPools(pool_names=["testpool"]), "item2"])
    task = ExecutorIterationTask(generator)

    with pytest.raises(PoolOverflowingException):
        list(task)

    pools.update_pools({"testpool": 1})

    assert list(task) == ["item2"]
