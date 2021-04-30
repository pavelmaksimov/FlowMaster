import datetime as dt

from utils import iter_period_from_range


def test_iter_period_from_range():
    tm = dt.timedelta(1)
    range = [dt.datetime(2021, 1, 1)]
    assert list(iter_period_from_range(range, tm)) == [
        (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1))
    ]
    assert list(iter_period_from_range(range, dt.timedelta(2))) == [
        (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1))
    ]

    range = [dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 2), dt.datetime(2021, 1, 3)]
    assert list(iter_period_from_range(range, tm)) == [
        (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 3))
    ]

    range = [dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 3)]
    assert list(iter_period_from_range(range, tm)) == [
        (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1)),
        (dt.datetime(2021, 1, 3), dt.datetime(2021, 1, 3)),
    ]

    range = []
    assert list(iter_period_from_range(range, tm)) == []


def test_iter_period_from_range_with_length():
    tm = dt.timedelta(1)
    range = [dt.datetime(2021, 1, 1)]
    assert list(iter_period_from_range(range, tm, length=2)) == [
        (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1))
    ]

    range = [dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 2), dt.datetime(2021, 1, 3)]
    assert list(iter_period_from_range(range, tm, length=1)) == [
        (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1)),
        (dt.datetime(2021, 1, 2), dt.datetime(2021, 1, 2)),
        (dt.datetime(2021, 1, 3), dt.datetime(2021, 1, 3)),
    ]

    range = [dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 2), dt.datetime(2021, 1, 3)]
    assert list(iter_period_from_range(range, tm, length=2)) == [
        (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 2)),
        (dt.datetime(2021, 1, 3), dt.datetime(2021, 1, 3)),
    ]

    range = [dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 2), dt.datetime(2021, 1, 3)]
    assert list(iter_period_from_range(range, tm, length=3)) == [
        (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 3))
    ]
