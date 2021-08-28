import datetime as dt
import functools
import queue
import threading
import time
from typing import Optional, TypeVar, Union, Callable

import pendulum
from pydantic import BaseModel, PrivateAttr

from flowmaster.executors.exceptions import (
    ExpiredError,
    SoftTimeLimitError,
    SleepException,
    PoolOverflowingException,
)
from flowmaster.pool import pools
from flowmaster.utils.logging_helper import logger

task_queue = queue.Queue()
sleeptask_queue = queue.Queue()
threading_event = threading.Event()
threading_lock = threading.RLock()


def catch_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            ...

    return wrapper


class SleepIteration(BaseModel):
    sleep: int
    _time_begin: pendulum.datetime = PrivateAttr(default_factory=pendulum.now)
    _this_is_sleeptask = PrivateAttr(default=True)

    def reset(self):
        self._time_begin = pendulum.now()

    def allow(self) -> bool:
        return pendulum.now() > self._time_begin.add(seconds=self.sleep)

    def rest_of_sleep(self) -> int:
        a = (self._time_begin.add(seconds=self.sleep) - pendulum.now()).seconds
        return max(0, a)


class NextIterationInPools(BaseModel):
    pool_names: Optional[list[str]]
    sleep: int = 1
    _time_begin: pendulum.datetime = PrivateAttr(default_factory=pendulum.now)
    _this_is_taskpool = PrivateAttr(default=True)

    def allow(self) -> bool:
        return pools.allow(self.pool_names)

    def done(self) -> None:
        pools[self.pool_names] = -1

    def put(self) -> None:
        pools[self.pool_names] = 1

    def rest_of_sleep(self) -> int:
        a = (self._time_begin.add(seconds=self.sleep) - pendulum.now()).seconds
        return max(0, a)


AsyncIterationT = TypeVar(
    "AsyncIterationT", bound=Union[SleepIteration, NextIterationInPools]
)


class ExecutorIterationTask:
    begin_time: time = None
    duration: float = None
    execute_duration: float = None
    iteration_num = 0
    _sleep_item: Optional[AsyncIterationT] = None

    def __init__(
        self,
        iterator,
        /,
        *,
        expires: Optional[dt.datetime] = None,
        soft_time_limit_seconds: Optional[int] = None,
    ):
        self.iterator = iterator
        self.soft_time_limit_seconds = soft_time_limit_seconds
        self.expires = expires

    def rest_of_sleep(self) -> Optional[int]:
        if self._sleep_item is not None:
            return self._sleep_item.rest_of_sleep()

    def allow(self) -> bool:
        if self._sleep_item is not None:
            return self._sleep_item.allow()
        return True

    def check_limit(self) -> None:
        if self.expires is not None and pendulum.now("UTC") > self.expires:
            raise ExpiredError(
                f"{self.duration=}, {self.execute_duration=}, {self.expires=}"
            )

        if (
            self.soft_time_limit_seconds
            and self.execute_duration
            and (self.execute_duration > self.soft_time_limit_seconds)
        ):
            raise SoftTimeLimitError(
                f"{self.execute_duration=}, {self.duration=}, {self.expires=}"
            )

    def _iterate(self):
        if self._sleep_item is None:
            result = next(self.iterator)
        else:
            result = self._sleep_item
            self._sleep_item = None

        if getattr(result, "_this_is_taskpool", False):
            with threading_lock:
                pool: NextIterationInPools = result
                if not pool.allow():
                    self._sleep_item = pool
                    raise PoolOverflowingException(sleep=pool.sleep)
                else:
                    pool.put()

            try:
                self._sleep_item = None
                return self._iterate()
            finally:
                pool.done()

        elif getattr(result, "_this_is_sleeptask", False):
            sleep_iteration: SleepIteration = result
            if not sleep_iteration.allow():
                self._sleep_item = sleep_iteration
                raise SleepException(sleep=sleep_iteration.rest_of_sleep())
            else:
                self._sleep_item = None
                return self._iterate()

        return result

    def __iter__(self):
        return self

    def __next__(self):
        if self.iteration_num == 0:
            self.begin_time = time.time()

        self.check_limit()
        iteration_begin_time = time.time()
        try:
            return self._iterate()
        finally:
            self.iteration_num += 1
            self.execute_duration = time.time() - iteration_begin_time
            self.duration = time.time() - self.begin_time
            self.check_limit()

    def execute(self) -> list:
        """
        Executes the flow immediately, bypassing the executor and the scheduler.
        All restrictions will be taken into account.
        """

        def _():
            while True:
                try:
                    yield next(self)
                except SleepException as sleep_iteration:
                    time.sleep(sleep_iteration.sleep)
                except StopIteration:
                    break

        return list(_())


class ThreadAsyncExecutor:
    def __init__(self, ordering_task_func: Callable, *, dry_run: bool = False):
        self.order_task_func = ordering_task_func
        self.dry_run = dry_run
        self.sleeping_task_storage: list[ExecutorIterationTask] = []
        self.threads = {}

    def wake_sleep_func(self) -> None:
        """Get the task out of sleep and add it to the queue."""
        new_sleeping_storage = []
        while self.sleeping_task_storage:
            sleep_task = self.sleeping_task_storage.pop(0)
            sleep_task: ExecutorIterationTask
            if sleep_task.allow():
                sleeptask_queue.put(sleep_task)
            else:
                new_sleeping_storage.append(sleep_task)

        self.sleeping_task_storage = new_sleeping_storage

    def fill_queue(self) -> None:
        """Adds new function to the queue"""
        count = 0
        with threading_lock:
            for task in self.order_task_func():
                task: ExecutorIterationTask
                task_queue.put(task)
                count += 1

        logger.info("Count ordering task: {}", count)

    def get_task(self) -> tuple[queue.Queue, ExecutorIterationTask]:
        while not threading_event.is_set():
            try:
                # Если убрать задержку, то спящие таски плохо поднимаются в очередь.
                task: ExecutorIterationTask = sleeptask_queue.get(
                    timeout=0 if self.dry_run else 1
                )
            except queue.Empty:
                ...
            else:
                return sleeptask_queue, task

            try:
                task: ExecutorIterationTask = task_queue.get_nowait()
            except queue.Empty:
                ...
            else:
                return task_queue, task

    def worker(self) -> None:
        logger.info("Start worker")
        try:
            while not threading_event.is_set():
                queue_and_task = self.get_task()
                if queue_and_task:
                    queue_, task = queue_and_task
                    try:
                        list(task)
                    except SleepException:
                        self.sleeping_task_storage.append(task)
                    except Exception as exc:
                        logger.error("Fail task: {}", exc)
                    finally:
                        queue_.task_done()
        except:
            logger.exception("Fail worker")
        finally:
            logger.info("Stop worker")

    def create_worker_in_thread(self, number: int) -> None:
        for n in range(number):
            name = f"FlowMaster_worker{n + 1}"
            worker_thread = threading.Thread(
                target=self.worker,
                name=name,
                daemon=True,
            )
            self.threads[name] = worker_thread
            worker_thread.start()

    def restart_stopped_workers(self) -> None:
        for thread in threading.enumerate():
            if thread.name in self.threads and not thread.is_alive():
                worker = self.threads[thread.name]
                worker.start()

    def run_continuously(
        self, order_interval: int = 15, orders: int = None, work_duration: int = None
    ) -> None:
        def scheduler():
            logger.info("Start scheduler")
            begin = time.time()
            iter_begin = time.time()
            duration = order_interval
            num_order = 0

            while not threading_event.is_set() and (
                work_duration is None or time.time() - begin < work_duration
            ):
                self.wake_sleep_func()

                if duration >= order_interval:
                    duration = 0
                    iter_begin = time.time()

                    logger.info("Pool info: {}", pools.info_text())
                    logger.info(
                        "The number of new tasks in the queue: {}", task_queue.qsize()
                    )
                    logger.info(
                        "Number of sleeping tasks in the queue: {}",
                        sleeptask_queue.qsize(),
                    )

                    if orders is None or num_order < orders:
                        self.fill_queue()
                        num_order += 1

                time.sleep(0 if self.dry_run else 1)
                duration += time.time() - iter_begin

            logger.info("Stop scheduler")

        schedule_thread = threading.Thread(
            target=scheduler,
            name="Flowmaster_scheduler",
        )
        self.threads["schedule_thread"] = schedule_thread
        schedule_thread.start()

    def start(
        self,
        workers: int = 1,
        interval: Union[int, float] = 15,
        orders: int = None,
        work_duration: int = None,
    ) -> None:
        self.create_worker_in_thread(workers)
        self.run_continuously(interval, orders, work_duration)

    def stop(self) -> None:
        threading_event.set()
        [thread.join() for thread in self.threads.values()]
