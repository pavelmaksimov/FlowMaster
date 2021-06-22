import datetime as dt
from typing import Iterator, Optional, TYPE_CHECKING

import pendulum

from flowmaster.models import FlowItem
from flowmaster.utils import iter_range_datetime, iter_period_from_range
from flowmaster.utils.logging_helper import Logger, getLogger

if TYPE_CHECKING:
    from flowmaster.operators.base.policy import Notebook
    from flowmaster.executors import ExecutorIterationTask


class Work:
    def __init__(self, notebook: "Notebook", logger: Optional[Logger] = None):
        self.notebook = notebook
        self.name = notebook.name
        self.interval_timedelta = notebook.work.triggers.schedule._interval_timedelta
        self.is_second_interval = notebook.work.triggers.schedule._is_second_interval
        self.period_length = notebook.work.triggers.schedule.period_length
        self.timezone = notebook.work.triggers.schedule.timezone
        self.start_datetime = notebook.work.triggers.schedule._start_datetime
        self.keep_sequence = notebook.work.triggers.schedule.keep_sequence
        self.retry_delay = notebook.work.retry_delay
        self.retries = notebook.work.retries
        self.max_fatal_errors = notebook.work.max_fatal_errors
        self.soft_time_limit_seconds = notebook.work.soft_time_limit_seconds
        if notebook.work.time_limit_seconds_from_worktime is not None:
            self.expires = self.current_worktime + dt.timedelta(
                seconds=notebook.work.time_limit_seconds_from_worktime
            )
        elif self.is_second_interval:
            self.expires = self.current_worktime + dt.timedelta(
                seconds=self.interval_timedelta.total_seconds()
            )
        else:
            self.expires = None

        self.Model = FlowItem
        self.logger = logger or getLogger()

        self.concurrency_pool_names = notebook.work.pools or []
        if self.notebook.work.concurrency is not None:
            self.concurrency_pool_names.append(f"__{self.name}_concurrency__")
            self.add_pool(
                f"__{self.name}_concurrency__", self.notebook.work.concurrency
            )

    def add_pool(self, name: str, limit: int) -> None:
        from flowmaster.pool import pools

        pools.update_pools({name: limit})

    @property
    def current_worktime(self) -> dt.datetime:
        """
        Returns the work datetime at the moment.
        """
        end_time = pendulum.now(self.timezone)

        if self.start_datetime > end_time:
            start_time = self.start_datetime - self.interval_timedelta
        else:
            start_time = self.start_datetime

        worktime = list(
            iter_range_datetime(
                start_time=start_time,
                end_time=end_time,
                timedelta=self.interval_timedelta,
            )
        )[-1]

        if self.is_second_interval is False:
            worktime = worktime - self.interval_timedelta

        return worktime

    def iter_items_for_execute(self) -> Iterator[FlowItem]:
        """
        Collects all flow items for execute.
        """
        return self.Model.get_items_for_execute(
            self.name,
            self.current_worktime,
            self.start_datetime,
            self.interval_timedelta,
            self.keep_sequence,
            self.retries,
            self.retry_delay,
            notebook_hash=self.notebook.hash,
            max_fatal_errors=self.max_fatal_errors,
        )

    def iter_datetime_for_execute(self) -> Iterator[dt.datetime]:
        """
        Collects all work datetimes for execute.
        """
        for item in self.iter_items_for_execute():
            yield item.worktime

    def iter_period_for_execute(self) -> Iterator[tuple[dt.datetime, dt.datetime]]:
        """
        Collects all work datetimes for execute.
        """
        datetime_range = [item.worktime for item in self.iter_items_for_execute()]
        yield from iter_period_from_range(
            datetime_range, self.interval_timedelta, self.period_length
        )


def ordering_flow_tasks(*, dry_run: bool = False) -> Iterator["ExecutorIterationTask"]:
    from flowmaster.operators.etl.work import ordering_etl_flow_tasks

    yield from ordering_etl_flow_tasks(dry_run=dry_run)
