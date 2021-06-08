import datetime as dt
from logging import Logger, getLogger
from typing import Iterator, Optional, TYPE_CHECKING

import pendulum

from flowmaster.models import FlowItem
from flowmaster.utils import iter_range_datetime, iter_period_from_range

if TYPE_CHECKING:
    from flowmaster.operators.base.policy import FlowConfig
    from flowmaster.executors import ExecutorIterationTask


class Work:
    def __init__(self, config: "FlowConfig", logger: Optional[Logger] = None):
        self.config = config
        self.name = config.name
        self.interval_timedelta = config.work.triggers.schedule._interval_timedelta
        self.is_second_interval = config.work.triggers.schedule._is_second_interval
        self.period_length = config.work.triggers.schedule.period_length
        self.timezone = config.work.triggers.schedule.timezone
        self.start_datetime = config.work.triggers.schedule._start_datetime
        self.keep_sequence = config.work.triggers.schedule.keep_sequence
        self.retry_delay = config.work.retry_delay
        self.retries = config.work.retries
        self.soft_time_limit_seconds = config.work.soft_time_limit_seconds
        if config.work.time_limit_seconds_from_worktime is not None:
            self.expires = self.current_worktime + dt.timedelta(
                seconds=config.work.time_limit_seconds_from_worktime
            )
        elif self.is_second_interval:
            self.expires = self.current_worktime + dt.timedelta(
                seconds=self.interval_timedelta.total_seconds()
            )
        else:
            self.expires = None

        self.Model = FlowItem
        self.logger = logger or getLogger(__name__)

        self.concurrency_pool_names = config.work.pools or []
        if self.config.work.concurrency is not None:
            self.concurrency_pool_names.append(f"__{self.name}_concurrency__")
            self.add_pool(f"__{self.name}_concurrency__", self.config.work.concurrency)

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
            config_hash="",
            max_fatal_errors=3,
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


def ordering_flow_tasks(
    *, logger: Logger, dry_run: bool = False, **kwargs
) -> Iterator["ExecutorIterationTask"]:
    from flowmaster.operators.etl.work import ordering_etl_flow_tasks

    yield from ordering_etl_flow_tasks(logger=logger, dry_run=dry_run, **kwargs)
