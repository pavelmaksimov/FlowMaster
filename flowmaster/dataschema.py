import abc
import datetime as dt
from typing import Optional

import pydantic


class OrmDataAbstract(pydantic.BaseModel, abc.ABC):
    class Config:
        orm_mode = True

    def save(self):
        from flowmaster.models import FlowItem

        FlowItem(**self.dict(exclude_unset=True)).save()


class FlowItemData(OrmDataAbstract):
    name: pydantic.constr(max_length=255)
    worktime: dt.datetime
    operator: pydantic.constr(max_length=255)
    updated: dt.datetime

    status: Optional[pydantic.constr(max_length=255)] = None
    data: dict = pydantic.Field(default_factory=dict)
    retries: pydantic.PositiveInt = 0
    duration: Optional[pydantic.PositiveInt] = None
    log: Optional[str] = None
    started_utc: Optional[dt.datetime] = None
    finished_utc: Optional[dt.datetime] = None
