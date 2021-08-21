import datetime as dt
from functools import partial
from typing import Union, Iterable, Optional, Sequence

import orjson
import peewee
import pendulum
import playhouse.sqlite_ext
import pydantic
from playhouse.hybrid import hybrid_property

from flowmaster.database import db
from flowmaster.enums import Statuses
from flowmaster.utils import iter_period_from_range, iter_range_datetime, custom_encoder
from flowmaster.utils.logging_helper import logger


class DateTimeTZField(playhouse.sqlite_ext.DateTimeField):
    def python_value(self, value: str) -> dt.datetime:
        dt_str, tz_name = value.split(" ")
        return pendulum.parse(dt_str, tz=tz_name)

    def db_value(
        self, value: Optional[Union[dt.datetime, pendulum.DateTime]]
    ) -> Optional[str]:
        if value is not None:
            if not isinstance(value, pendulum.DateTime):
                value = pendulum.instance(value)

            dt_str = value.replace(tzinfo=None).isoformat()

            return f"{dt_str} {value.timezone_name}"


class DateTimeUTCField(playhouse.sqlite_ext.DateTimeField):
    def python_value(self, value: str) -> pendulum.DateTime:
        if value is not None:
            return pendulum.parse(value, tz=pendulum.timezone("UTC"))

    def db_value(
        self, value: Optional[Union[dt.datetime, pendulum.DateTime]]
    ) -> Optional[str]:
        if value is not None:
            if value.tzinfo is None:
                raise ValueError(f"{value} timezone not set.")

            value = value.astimezone(pendulum.timezone("UTC"))
            value = value.replace(tzinfo=None).isoformat()

        return value


class BaseDBModel(playhouse.sqlite_ext.Model):
    class Meta:
        database = db


class FlowItem(BaseDBModel):
    """When changing fields, update to operators.etl.providers.flowmaster_data.policy.FlowmasterDataExportPolicy"""

    name = playhouse.sqlite_ext.CharField()
    worktime = DateTimeTZField()

    status = playhouse.sqlite_ext.CharField(default=Statuses.add, null=False)
    data = playhouse.sqlite_ext.JSONField(
        default={},
        json_dumps=partial(orjson.dumps, default=custom_encoder),
        json_loads=orjson.loads,
    )
    notebook_hash = playhouse.sqlite_ext.CharField(default="", null=False)
    retries = playhouse.sqlite_ext.IntegerField(default=0)
    duration = playhouse.sqlite_ext.IntegerField(null=True)
    info = playhouse.sqlite_ext.TextField(null=True)
    logpath = playhouse.sqlite_ext.TextField(null=True)
    expires_utc = DateTimeUTCField(null=True)
    started_utc = DateTimeUTCField(null=True)
    finished_utc = DateTimeUTCField(null=True)
    created_utc = DateTimeUTCField(default=pendulum.now("UTC"))
    updated_utc = DateTimeUTCField(default=pendulum.now("UTC"))

    class Meta:
        primary_key = playhouse.sqlite_ext.CompositeKey("name", "worktime")

    @hybrid_property
    def worktime_for_url(self):
        self.worktime: dt.datetime
        tz_name = pendulum.instance(self.worktime).timezone_name.replace("/", ".")
        worktime_str = self.worktime.strftime("%Y-%m-%dT%H:%M:%S")
        return f"{worktime_str}Z{tz_name}"

    @classmethod
    def worktime_from_url(cls, worktime_for_url):
        datetime, tz_name = worktime_for_url.split("Z")
        tz_name = tz_name.replace(".", "/")
        worktime = pendulum.parse(datetime, tz=tz_name)
        return worktime

    @classmethod
    def count_items(
        cls, flow_name: str, statuses: Optional[list[Statuses.LiteralT]] = None
    ) -> int:
        query = cls.select().where(cls.name == flow_name)
        if statuses is not None:
            query = query.where(cls.status.in_(statuses))
        return query.count()

    @classmethod
    def count_items_by_name(cls) -> peewee.ModelSelect:
        query = cls.select(
            cls.name, peewee.fn.Count(cls.worktime).alias("count")
        ).group_by(cls.name)
        return query

    @classmethod
    def count_items_by_name_and_status(cls) -> peewee.ModelSelect:
        query = cls.select(
            cls.name, cls.status, peewee.fn.Count(cls.worktime).alias("count")
        ).group_by(cls.name, cls.status)
        return query

    @classmethod
    def exists(cls, flow_name: str) -> bool:
        return bool(cls.get_or_none(**{cls.name.name: flow_name}))

    @classmethod
    def first_item(cls, flow_name: str) -> Optional["FlowItem"]:
        items = cls.select().where(cls.name == flow_name)
        if items:
            return items.get()

    @classmethod
    def last_item(
        cls, flow_name: str, *, for_updated: bool = False
    ) -> Optional["FlowItem"]:
        items = cls.select().where(cls.name == flow_name)

        if for_updated:
            items = items.order_by(cls.worktime.desc())
        else:
            items = items.order_by(cls.updated_utc.desc())

        if items:
            return items.get()

    @classmethod
    def change_status(
        cls,
        flow_name: str,
        new_status: Statuses.LiteralT,
        *,
        filter_statuses: Optional[tuple[Statuses.LiteralT]] = None,
        from_time: Optional[dt.datetime] = None,
        to_time: Optional[dt.datetime] = None,
    ) -> int:
        query = cls.update(**{cls.status.name: new_status}).where(cls.name == flow_name)

        if filter_statuses:
            query = query.where(cls.status.in_(filter_statuses))

        if from_time:
            query = query.where(cls.worktime >= from_time)

        if to_time:
            query = query.where(cls.worktime <= to_time)

        return query.execute()

    @classmethod
    def change_expires(
        cls,
        flow_name: str,
        expires: pendulum.DateTime,
        *,
        from_time: dt.datetime,
        to_time: dt.datetime,
    ) -> int:
        query = cls.update(**{cls.expires_utc.name: expires}).where(
            cls.name == flow_name, cls.worktime >= from_time, cls.worktime <= to_time
        )

        return query.execute()

    @classmethod
    def recreate_item(
        cls,
        flow_name: str,
        worktime: dt.datetime,
    ) -> list["FlowItem"]:
        cls.clear(flow_name, worktime, worktime)
        return cls.create_items(flow_name, [worktime])

    @classmethod
    def recreate_items(
        cls,
        flow_name: str,
        filter_statuses: Optional[tuple[Statuses.LiteralT]] = None,
        from_time: Optional[dt.datetime] = None,
        to_time: Optional[dt.datetime] = None,
    ) -> list["FlowItem"]:
        query = cls.select().where(cls.name == flow_name)
        if from_time:
            query = query.where(cls.worktime >= from_time)

        if to_time:
            query = query.where(cls.worktime <= to_time)

        if filter_statuses:
            query = query.where(cls.status.in_(filter_statuses))

        worktime_list = [i.worktime for i in query]
        cls.clear(flow_name, from_time, to_time)

        return cls.create_items(flow_name, worktime_list)

    @classmethod
    def is_create_next(
        cls,
        flow_name: str,
        interval_timedelta: dt.timedelta,
        worktime: dt.datetime,
    ) -> bool:
        last_executed_item = cls.last_item(flow_name, for_updated=True)
        return (
            last_executed_item
            and worktime - last_executed_item.worktime >= interval_timedelta
        )

    @classmethod
    def clear(
        cls,
        flow_name: str,
        from_time: Optional[dt.datetime] = None,
        to_time: Optional[dt.datetime] = None,
    ) -> int:
        query = cls.delete().where(cls.name == flow_name)
        if from_time:
            query = query.where(cls.worktime >= from_time)

        if to_time:
            query = query.where(cls.worktime <= to_time)

        return query.execute()

    @classmethod
    def create_next_execute_item(
        cls,
        flow_name: str,
        interval_timedelta: dt.timedelta,
        worktime: dt.datetime,
    ) -> Optional["FlowItem"]:
        if cls.is_create_next(flow_name, interval_timedelta, worktime):
            last_executed_item = cls.last_item(flow_name, for_updated=True)
            next_worktime = last_executed_item.worktime + interval_timedelta
            try:
                item = cls.create(
                    **{
                        cls.name.name: flow_name,
                        cls.worktime.name: next_worktime,
                    }
                )
            except peewee.IntegrityError:
                return None
            else:
                logger.info("Created next worktime {} for {}", next_worktime, flow_name)
                return item

    @classmethod
    def create_missing_items(
        cls,
        flow_name: str,
        start_time: dt.datetime,
        end_time: dt.datetime,
        interval_timedelta: dt.timedelta,
    ) -> list["FlowItem"]:
        items = []
        for datetime_ in iter_range_datetime(start_time, end_time, interval_timedelta):
            try:
                item = cls.create(
                    **{cls.name.name: flow_name, cls.worktime.name: datetime_}
                )
            except peewee.IntegrityError:
                item = cls.get(cls.name == flow_name, cls.worktime == datetime_)
            else:
                logger.info("Created missing worktime {} for {}", datetime_, flow_name)

            items.append(item)

        return items

    @classmethod
    def recreate_prev_items(
        cls,
        flow_name: str,
        worktime: dt.datetime,
        offset_periods: Union[pydantic.PositiveInt, list[pydantic.NegativeInt]],
        interval_timedelta: dt.timedelta,
    ) -> Optional[list["FlowItem"]]:

        if isinstance(offset_periods, int):
            if offset_periods > 0:
                offset_periods = [-i for i in range(offset_periods) if i > 0]
            else:
                raise ValueError("Only positive Int")
        else:
            assert all([i < 0 for i in offset_periods])

        first_item = cls.first_item(flow_name)
        if first_item:
            worktime_list = [
                worktime + (interval_timedelta * delta) for delta in offset_periods
            ]
            worktime_list = list(
                filter(lambda dt_: dt_ >= first_item.worktime, worktime_list)
            )

            cls.delete().where(
                cls.name == flow_name, cls.worktime.in_(worktime_list)
            ).execute()

            items = []
            for date1, date2 in iter_period_from_range(
                worktime_list, interval_timedelta
            ):
                new_items = cls.create_missing_items(
                    flow_name,
                    start_time=date1,
                    end_time=date2,
                    interval_timedelta=interval_timedelta,
                )
                items.extend(new_items)

            logger.info(
                "Recreated items to restart flows {} for previous worktimes {}",
                flow_name,
                worktime_list,
            )

            return items

    @classmethod
    def allow_execute_flow(
        cls, flow_name: str, notebook_hash: str, *, max_fatal_errors: int = 3
    ) -> bool:
        item = cls.last_item(flow_name, for_updated=True)

        if item and item.notebook_hash == notebook_hash:
            # Check limit fatal errors.
            items = (
                cls.select()
                .where(cls.name == flow_name, cls.status == Statuses.fatal_error)
                .order_by(cls.updated_utc.desc())
                .limit(max_fatal_errors)
            )
            is_allow = len(items) < max_fatal_errors
            if not is_allow:
                logger.info("Many fatal errors, {} will not be scheduled", flow_name)

            return is_allow
        else:
            return True

    @classmethod
    def retry_error_items(
        cls, flow_name: str, retries: int, retry_delay: int
    ) -> peewee.ModelSelect:
        # http://docs.peewee-orm.com/en/latest/peewee/hacks.html?highlight=time%20now#date-math
        # A function that checks to see if retry_delay passes to restart.
        next_start_time_timestamp = cls.finished_utc.to_timestamp() + retry_delay
        items = cls.select().where(
            cls.name == flow_name,
            cls.status.in_(Statuses.error_statuses),
            cls.retries < retries,
            (
                (pendulum.now("UTC").timestamp() >= next_start_time_timestamp)
                | (cls.finished_utc.is_null())
            ),
            # TODO: В поле info записывать, что поток не будет перезапущен, т.к. истек срок выполнения.
            #  Иначе не понятно, почему не перезапускаются.
            (
                (pendulum.now("UTC").timestamp() <= cls.expires_utc.to_timestamp())
                | (cls.expires_utc.is_null())
            ),
        )
        worktimes = [i.worktime for i in items]

        if worktimes:
            # TODO: recreate items
            cls.update(
                **{
                    cls.status.name: Statuses.add,
                    cls.retries.name: cls.retries + 1,
                    cls.updated_utc.name: pendulum.now("UTC"),
                }
            ).where(cls.name == flow_name, cls.worktime.in_(worktimes)).execute()

            logger.info(
                "Restart error items for {}, worktimes = {}", flow_name, worktimes
            )

        return (
            cls.select()
            .where(cls.name == flow_name, cls.worktime.in_(worktimes))
            .order_by(cls.worktime.desc())
        )

    @classmethod
    def get_items_for_execute(
        cls,
        flow_name: str,
        worktime: dt.datetime,
        start_time: dt.datetime,
        interval_timedelta: dt.timedelta,
        keep_sequence: bool,
        retries: int,
        retry_delay: int,
        notebook_hash: str,
        max_fatal_errors: int,
        update_stale_data: Optional[
            Union[pydantic.PositiveInt, list[pydantic.NegativeInt]]
        ] = None,
    ) -> Optional[list["FlowItem"]]:
        if cls.allow_execute_flow(
            flow_name, notebook_hash=notebook_hash, max_fatal_errors=max_fatal_errors
        ):
            if not cls.exists(flow_name):
                cls.create(**{cls.name.name: flow_name, cls.worktime.name: worktime})
                logger.info(
                    "Created first item for {}, worktime {}", flow_name, worktime
                )

            if cls.create_next_execute_item(flow_name, interval_timedelta, worktime):
                if update_stale_data:
                    # When creating the next item, elements are created to update the data for the past dates.
                    cls.recreate_prev_items(
                        flow_name, worktime, update_stale_data, interval_timedelta
                    )

            if keep_sequence:
                cls.create_missing_items(
                    flow_name, start_time, worktime, interval_timedelta
                )

            cls.retry_error_items(flow_name, retries, retry_delay)

            return (
                cls.select()
                .where(
                    cls.name == flow_name,
                    cls.status == Statuses.add,
                    (
                        (
                            pendulum.now("UTC").timestamp()
                            <= cls.expires_utc.to_timestamp()
                        )
                        | (cls.expires_utc.is_null())
                    ),
                )
                .order_by(cls.worktime.desc())
            )

    @classmethod
    def clear_statuses_of_lost_items(cls) -> None:
        cls.update(
            **{cls.status.name: Statuses.error, cls.info.name: "ExpiredError"}
        ).where(
            pendulum.now("UTC").timestamp() >= cls.expires_utc.to_timestamp()
        ).execute()

        cls.update(**{cls.status.name: Statuses.add}).where(
            cls.status.in_([Statuses.run])
        ).execute()

    @classmethod
    def create_items(
        cls, flow_name: str, worktime_list: Iterable[dt.datetime], **kwargs
    ) -> list["FlowItem"]:
        items = []
        for datetime_ in worktime_list:
            try:
                item = cls.create(
                    **{
                        cls.name.name: flow_name,
                        cls.worktime.name: datetime_,
                        **kwargs,
                    },
                )
            except peewee.IntegrityError:
                item = cls.get(cls.name == flow_name, cls.worktime == datetime_)
                cls.update_items([item], **kwargs)

            items.append(item)

        return items

    @classmethod
    def update_items(
        cls,
        items: list["FlowItem"],
        save: bool = True,
        save_expression_fields: bool = True,
        **kwargs,
    ) -> dict:
        kwargs.update({cls.updated_utc.name: pendulum.now("UTC")})
        not_saved_data = {}

        for item in items:
            for field, value in kwargs.items():
                if field in cls.__dict__:
                    if (
                        isinstance(value, peewee.Expression)
                        and save_expression_fields is False
                    ):
                        # The values of the expression (peewee.Expression) are triggered on every save,
                        # to avoid this, they will be returned to be updated later.
                        not_saved_data[field] = value
                        continue

                    setattr(item, field, value)
            if save:
                item.save()

        return not_saved_data

    @classmethod
    def iter_items(
        cls,
        flow_name: str,
        statuses: Optional[Sequence[Statuses.LiteralT]] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> peewee.ModelSelect:
        query = (
            FlowItem.select()
            .where(FlowItem.name == flow_name)
            .order_by(cls.worktime.desc())
            .limit(limit)
            .offset(offset)
        )
        if statuses is not None:
            query = query.where(cls.status.in_(statuses))

        return query


db.create_tables([FlowItem])
