import pytest

from flowmaster.pool import Counter, Pool


def test_counter():
    counter = Counter()
    counter["one"] = 1

    with pytest.raises(Exception):
        try:
            try:
                1 / 0
            finally:
                counter["one"] = -1
        except Exception:
            raise

    assert counter["one"] == 0


def test_pool():
    pools = Pool({"database_ch": 5, "yandex": 3, "google": 1})
    pools[["yandex", "database_ch"]] = 1

    assert pools["yandex"] == 1
    assert pools["database_ch"] == 1
    assert pools["google"] == 0

    pools[["yandex", "database_ch"]] = -1

    assert pools["yandex"] == 0
    assert pools["database_ch"] == 0
    assert pools["google"] == 0

    pools[["yandex", "database_ch"]] = -1

    assert pools["yandex"] == 0
    assert pools["database_ch"] == 0
    assert pools["google"] == 0

    # update pools
    pools.update_pools({"yandex": 100, "export": 10})

    assert pools.limits["yandex"] == 100
    assert pools.limits["export"] == 10
    assert pools.limits["database_ch"] == 5
    assert pools.limits["google"] == 1


def test_pool_allow():
    pools = Pool({"one": 1, "two": 2})

    pools[["one"]] = 1
    pools[["two"]] = 1

    assert not pools.allow(["one"])
    assert pools.allow(["two"])
    assert not pools.allow(["one", "two"])


def test_pool_uniq_name():
    pools = Pool({"one": 1, "two": 2})
    pools.append_pools({"one": 1, "two": 2})

    assert set(pools.limits.keys()) == {"one", "one_", "two", "two_"}
    assert set(pools.sizes.keys()) == {"one", "one_", "two", "two_"}
