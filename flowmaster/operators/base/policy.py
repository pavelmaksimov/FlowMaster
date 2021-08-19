import datetime as dt
from abc import ABC
from typing import Optional, Union, Literal, TypeVar

import pendulum
from pydantic import BaseModel, PositiveInt, PrivateAttr, validator

PydanticModelT = TypeVar("PydanticModelT", bound=BaseModel)


class BasePolicy(BaseModel):
    pools: Optional[list[str]] = None
    concurrency: Optional[int] = None


class BaseNotificationServicePolicy(BaseModel):
    on_retry: bool = False
    on_success: bool = False
    on_failure: bool = True


class _SchedulePolicy(BaseModel):
    interval: Union[PositiveInt, Literal["daily", "hourly"]]
    timezone: str
    start_time: str
    from_date: Optional[Union[str, pendulum.DateTime, dt.date, dt.datetime]] = None
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

    @validator("from_date", pre=True)
    def _validate_from_date(
        cls, from_date: Optional[Union[str, pendulum.DateTime]], values, **kwargs
    ) -> Optional[pendulum.DateTime]:
        if (
            isinstance(from_date, pendulum.DateTime)
            and from_date.timezone_name != values["timezone"]
        ):
            from_date = from_date.astimezone(pendulum.timezone(values["timezone"]))
        elif isinstance(from_date, str):
            from_date = pendulum.parse(from_date, tz=values["timezone"])
        elif isinstance(from_date, dt.date):
            from_date = pendulum.parse(
                from_date.strftime("%Y-%m-%dT%H:%M:%S"), tz=values["timezone"]
            )
        elif isinstance(from_date, dt.datetime):
            from_date = pendulum.instance(from_date, tz=values["timezone"])

        if from_date and not isinstance(from_date, pendulum.DateTime):
            raise TypeError(f"{from_date=} is type {type(from_date)}")

        return from_date

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
            self._start_datetime = self.from_date.replace(
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


class BaseNotebook(BaseModel, ABC):
    class WorkPolicy(BasePolicy):
        class TriggersPolicy(BaseModel):
            class SchedulePolicy(_SchedulePolicy):
                ...

            schedule: SchedulePolicy

        class NotificationsPolicy(BaseModel):
            class CodexTelegramPolicy(BaseNotificationServicePolicy):
                links: list[str]

            codex_telegram: CodexTelegramPolicy = None

        triggers: TriggersPolicy
        notifications: Optional[NotificationsPolicy]
        retries: int = 0
        retry_delay: int = 60
        time_limit_seconds_from_worktime: Optional[int] = None
        soft_time_limit_seconds: Optional[int] = None
        max_fatal_errors: int = 3

    name: str
    description: Optional[str] = None
    work: WorkPolicy
    hash: str = ""
    operator: str = "base"

    class Config:
        operator = "base"

    def __init__(self, **kwargs):
        super(BaseNotebook, self).__init__(**kwargs)
        if not getattr(self.Config, "operator"):
            raise AttributeError(
                "Assign to your model in the Config class, the 'operator' attribute"
            )
        self.operator = self.Config.operator
