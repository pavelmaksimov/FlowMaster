import datetime as dt
from logging import Logger, getLogger
from typing import Iterator, Optional, TYPE_CHECKING

import pendulum

from flowmaster.models import FlowItem
from flowmaster.utils import iter_range_datetime, iter_period_from_range

if TYPE_CHECKING:
    from flowmaster.operators.base.policy import BaseFlowConfig


class Work:
    def __init__(self, config: "BaseFlowConfig", logger: Optional[Logger] = None):
        self.config = config
        self.name = config.name
        self.interval_timedelta = config.work.schedule._interval_timedelta
        self.is_second_interval = config.work.schedule._is_second_interval
        self.period_length = config.work.schedule.period_length
        self.timezone = config.work.schedule.timezone
        self.start_datetime = config.work.schedule._start_datetime
        self.keep_sequence = config.work.schedule.keep_sequence
        self.retry_delay = config.work.retry_delay
        self.retries = config.work.retries

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
        end_time = pendulum.now(self.timezone).at(
            hour=self.start_datetime.hour,
            minute=self.start_datetime.minute,
            second=self.start_datetime.second,
        )
        if self.is_second_interval is False:
            end_time = end_time - self.interval_timedelta

        if self.start_datetime > end_time:
            start_time = self.start_datetime - self.interval_timedelta
        else:
            start_time = self.start_datetime

        return list(
            iter_range_datetime(
                start_time=start_time,
                end_time=end_time,
                timedelta=self.interval_timedelta,
            )
        )[-1]

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


def order_flow(
    *, logger: Logger, async_mode: bool = False, dry_run: bool = False, **kwargs
) -> Iterator:
    from flowmaster.operators.etl.work import order_etl_flow

    yield from order_etl_flow(
        logger=logger, async_mode=async_mode, dry_run=dry_run, **kwargs
    )
