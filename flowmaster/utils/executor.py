import datetime as dt
import functools
import logging
import queue
import threading
import time
from typing import Optional, Iterator, Callable, Union

from pydantic import BaseModel, PrivateAttr

from flowmaster.pool import pools
from flowmaster.utils.logging_helper import CreateLogger

logger = CreateLogger("executor", "executor.log", level=logging.INFO)

task_queue = queue.Queue()
sleeptask_queue = queue.Queue()


def catch_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            ...

    return wrapper


class SleepTask(BaseModel):
    sleep: int
    _generator: Optional[Iterator] = PrivateAttr()
    _time_begin: dt.datetime = PrivateAttr(default_factory=dt.datetime.utcnow)
    _this_is_sleeptask = PrivateAttr(default=True)

    def allow(self) -> bool:
        return dt.datetime.utcnow() > self._time_begin + dt.timedelta(
            seconds=self.sleep
        )

    def get_task(self):
        return self._generator


class TaskPool(BaseModel):
    pool_names: Optional[list[str]]
    _generator: Optional[Iterator] = PrivateAttr()
    _this_is_taskpool = PrivateAttr(default=True)

    def allow(self) -> bool:
        return pools.allow(self.pool_names)

    def get_task(self):
        return self

    def done(self):
        pools[self.pool_names] = -1

    def put(self):
        pools[self.pool_names] = 1


class Executor:
    def __init__(self, order_task_func: Callable = None, *, dry_run: bool = False):
        self.order_task_func = order_task_func
        self.sleeping_task_storage: list[Union[SleepTask, TaskPool]] = []
        self.lock = threading.Lock()
        self._worker_threads = {}
        self.dry_run = dry_run

    def wake_sleep_func(self) -> None:
        """Get the task out of sleep and add it to the queue."""
        with self.lock:
            new_sleeping_storage = []
            for sleep_task in self.sleeping_task_storage:
                if sleep_task.allow():
                    sleeptask_queue.put(sleep_task.get_task())
                else:
                    new_sleeping_storage.append(sleep_task)

            self.sleeping_task_storage = new_sleeping_storage

    def func_to_sleep(
        self, iterator_: Iterator, sleep_func: Union[SleepTask, TaskPool]
    ) -> None:
        """Adds a task (function) to the sleep storage."""
        sleep_func._generator = iterator_
        self.sleeping_task_storage.append(sleep_func)

    def fill_queue(self) -> None:
        """Adds new function to the queue"""
        count = 0
        with self.lock:
            for func in self.order_task_func(logger):
                task_queue.put(func)
                count += 1

        logger.info("Count ordering task: %s", count)

    def iterate(self, generator, result=None):
        while True:
            if getattr(generator, "_this_is_taskpool", False):
                result = generator
                generator = generator._generator

            if getattr(result, "_this_is_taskpool", False):
                with self.lock:
                    pool = result
                    if not pool.allow():
                        self.func_to_sleep(generator, result)
                        break
                    else:
                        pool.put()

                try:
                    result = next(generator)
                finally:
                    pool.done()

            elif getattr(result, "_this_is_sleeptask", False):
                sleep_task = result
                if sleep_task.allow():
                    result = next(generator)
                else:
                    self.func_to_sleep(generator, result)
                    break
            else:
                result = next(generator)

    def get_task(self):
        while True:
            try:
                # Если убрать задержку, то спящие таски плохо поднимаются в очередь.
                task = sleeptask_queue.get(timeout=0 if self.dry_run else 1)
            except queue.Empty:
                ...
            else:
                return sleeptask_queue, task

            try:
                task = task_queue.get_nowait()
            except queue.Empty:
                ...
            else:
                return task_queue, task

    def run_worker(self) -> None:
        logger_worker = CreateLogger("executor", "executor.log", level=logging.INFO)

        logger_worker.info("Start worker")
        try:
            while True:
                queue_, generator = self.get_task()
                try:
                    self.iterate(generator)
                except StopIteration:
                    ...
                except Exception as e:
                    logger_worker.error("Fail task: %s", e)
                finally:
                    queue_.task_done()

        except:
            logger_worker.exception("Fail worker")
        finally:
            logger_worker.info("Stop worker")

    def create_worker_in_thread(self, number: int) -> None:
        for n in range(number):
            name = f"FlowMaster worker{n + 1}"
            worker_thread = threading.Thread(
                target=self.run_worker, name=name, daemon=True
            )
            self._worker_threads[name] = worker_thread
            worker_thread.start()

    def restart_stopped_workers(self):
        for thread in threading.enumerate():
            if thread.name in self._worker_threads and not thread.is_alive():
                worker = self._worker_threads[thread.name]
                worker.start()

    def run_continuously(self, interval: int = 15, orders: int = None) -> None:
        """
        https://github.com/mrhwick/schedule/blob/master/schedule/__init__.py
        """
        logger.info("Start scheduler")

        cease_continuous_run = threading.Event()

        class ScheduleThread(threading.Thread):
            @classmethod
            def run(cls):
                begin = time.time()
                duration = interval
                num_order = 0

                while not cease_continuous_run.is_set():
                    self.wake_sleep_func()

                    if duration >= interval:
                        self.restart_stopped_workers()
                        duration = 0
                        begin = time.time()

                        if orders is None or num_order < orders:
                            self.fill_queue()
                            num_order += 1

                    time.sleep(1)
                    duration += time.time() - begin

        continuous_thread = ScheduleThread()
        continuous_thread.start()

    def start(
        self,
        workers: int = 1,
        order_interval: Union[int, float] = 15,
        orders: int = None,
    ) -> None:
        self.create_worker_in_thread(workers)
        if self.order_task_func:
            self.run_continuously(order_interval, orders)
