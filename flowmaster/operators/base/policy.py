import abc
import datetime as dt
from typing import Optional, Union, Literal

import pendulum
from pydantic import BaseModel, PositiveInt, PrivateAttr, validator


class BasePolicy(BaseModel):
    pools: Optional[list[str]] = None
    concurrency: Optional[int] = None


class BaseSchedulePolicy(BaseModel):
    interval: Union[PositiveInt, Literal["daily", "hourly"]]
    timezone: str
    start_time: str
    from_date: Optional[dt.date] = None
    period_length: int = 1
    keep_sequence: bool = False
    _start_datetime: dt.datetime = PrivateAttr()
    _is_second_interval: bool = PrivateAttr()
    _interval_timedelta: dt.timedelta = PrivateAttr()

    @validator("keep_sequence")
    def _validate_keep_sequence(cls, keep_sequence, values, **kwargs):
        if values.get("from_date") is None and keep_sequence is True:
            raise ValueError("keep_sequence cannot exist if from_date is missing")
        return keep_sequence

    def _set_keep_sequence(self):
        if self.from_date is not None:
            self.keep_sequence = True

    def _set_interval_timedelta(self):
        if isinstance(self.interval, int):
            self._interval_timedelta = dt.timedelta(seconds=self.interval)
        elif self.interval == "daily":
            self._interval_timedelta = dt.timedelta(days=1)
        elif self.interval == "hourly":
            self._interval_timedelta = dt.timedelta(hours=1)
        else:
            raise NotImplementedError(f"{self.interval} not supported")

    def _set_is_second_interval(self):
        self._is_second_interval = isinstance(self.interval, int)

    def _set_start_datetime(self):
        self._start_datetime = pendulum.parse(self.start_time, tz=self.timezone)

        if self.from_date is not None:
            self._start_datetime = pendulum.parse(
                self.from_date.isoformat(), tz=self.timezone
            ).replace(
                hour=self._start_datetime.hour,
                minute=self._start_datetime.minute,
                second=self._start_datetime.second,
            )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._set_start_datetime()
        self._set_is_second_interval()
        self._set_interval_timedelta()
        self._set_keep_sequence()


class NotificationServicePolicyAbstract(BaseModel, abc.ABC):
    on_retry: bool = False
    on_success: bool = False
    # on_failure: bool = True


class BaseWorkPolicy(BasePolicy):
    class Schedule(BaseSchedulePolicy):
        ...

    class Notifications(BaseModel):
        class CodexTelegram(NotificationServicePolicyAbstract):
            links: list[str]

        codex_telegram: CodexTelegram = None

    notifications: Optional[Notifications]
    schedule: BaseSchedulePolicy
    retries: int = 0
    retry_delay: int = 60
