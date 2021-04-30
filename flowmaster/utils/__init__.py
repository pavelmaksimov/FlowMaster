import datetime as dt
from copy import deepcopy
from itertools import islice
from typing import Iterable, Any, Optional


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
