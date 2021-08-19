import datetime as dt
from copy import deepcopy
from itertools import islice
from typing import Iterable, Any, Optional, TypeVar, Union


def chunker(iterable: Iterable, size: int) -> Any:
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


def iter_range_datetime(start_time, end_time, timedelta):
    i = 0
    date1 = deepcopy(start_time)
    while date1 <= end_time:
        i += 1
        yield date1
        date1 += timedelta
    if i == 0:
        yield date1


def iter_period_from_range(
    range: Iterable[dt.datetime],
    interval_timedelta: dt.timedelta,
    length: Optional[int] = None,
) -> list[tuple[dt.datetime, dt.datetime]]:
    range = sorted(set(range))
    while range:
        date1 = range.pop(0)
        date2 = date1
        i = 1
        while range:
            i += 1
            date = date2 + interval_timedelta
            if date in range and (length is None or i <= length):
                date2 = range.pop(range.index(date))
            else:
                break

        yield date1, date2


KlassT = TypeVar("KlassT", bound=type)
NameAttrOfKlassT = TypeVar("NameAttrOfKlassT", bound=str)
KlassNameT = TypeVar("KlassNameT", bound=str)


class KlassCollection:
    name_attr_of_klass = "name"

    def __init__(self, *klasses: tuple[KlassT]):
        self.__dict__["_name_attr_of_klasses"] = {}
        self._klasses = []
        for k in klasses:
            self.set(k)

    def __getattr__(self, name: KlassNameT) -> KlassT:
        if name in self.__dict__:
            return self.__dict__[name]
        raise AttributeError(name)

    def __iter__(self):
        self._it = iter(self._klasses)
        return self

    def __next__(self):
        return next(self._it)

    def __len__(self) -> int:
        return len(self._classes)

    def __contains__(self, name: Union[NameAttrOfKlassT, KlassNameT]) -> bool:
        return name in self.__dict__["_name_attr_of_klasses"] or self.__dict__

    def __call__(self, *klass_args, **klass_kwargs):
        name = klass_kwargs.get(self.name_attr_of_klass)
        if name is None:
            for arg in klass_args:
                if isinstance(arg, dict):
                    name = arg.get(self.name_attr_of_klass)
                else:
                    name = getattr(arg, self.name_attr_of_klass)

                if isinstance(name, str):
                    break
            else:
                raise AttributeError(self.name_attr_of_klass)

        return self[name](*klass_args, **klass_kwargs)

    def init(
        self, name: Union[NameAttrOfKlassT, KlassNameT], /, *klass_args, **klass_kwargs
    ):
        return self[name](*klass_args, **klass_kwargs)

    def __getitem__(self, name: Union[NameAttrOfKlassT, KlassNameT]) -> KlassT:
        if name in self.__dict__["_name_attr_of_klasses"] or self.__dict__:
            return self.__dict__.get(name, self.__dict__["_name_attr_of_klasses"][name])
        raise KeyError(name)

    def get(self, name: Union[NameAttrOfKlassT, KlassNameT], /) -> KlassT:
        return self[name]

    def set(self, klass: KlassT, /) -> None:
        if klass not in self._klasses:
            self._klasses.append(klass)
            self.__dict__[klass.__name__] = klass
            if hasattr(klass, "name"):
                self.__dict__["_name_attr_of_klasses"][klass.name] = klass

    def klass_names(self) -> list[str]:
        return [c.__name__ for c in self._klasses]

    def name_attr_of_klasses(self) -> list[str]:
        return list(self.__dict__["_name_attr_of_klasses"].keys())
