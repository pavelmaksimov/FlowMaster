import atexit

from playhouse.sqliteq import SqliteQueueDatabase

from flowmaster.setttings import Settings

db = SqliteQueueDatabase(
    # http://docs.peewee-orm.com/en/latest/peewee/playhouse.html#sqliteq
    Settings.APP_HOME / "db.sqlite_ext",
    pragmas=(
        ("cache_size", -1024 * 64),  # 64MB page-cache.
        ("journal_mode", "wal"),  # Use WAL-mode (you should always use this!).
        ("foreign_keys", 1),
    ),
    use_gevent=False,  # Use the standard library "threading" module.
    autostart=False,  # The worker thread now must be started manually.
    queue_max_size=64,  # Max. # of pending writes that can accumulate.
    results_timeout=3.0,  # Max. time to wait for query to be executed.
)


@atexit.register
def _stop_worker_threads():
    """http://docs.peewee-orm.com/en/latest/peewee/playhouse.html#sqliteq"""
    db.stop()
