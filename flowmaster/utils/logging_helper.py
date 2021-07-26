import atexit as _atexit
import sys as _sys
from pathlib import Path

from loguru import logger
from loguru._logger import Logger

from flowmaster.setttings import Settings


def create_logfile(filename: str, relative_path: str = None) -> Path:
    Path.mkdir(Settings.LOGS_DIR, exist_ok=True)
    path = Settings.LOGS_DIR

    if relative_path is not None:
        path = Settings.LOGS_DIR / relative_path

    Path.mkdir(path, exist_ok=True)

    return path / filename


def getLogger() -> Logger:
    from loguru._logger import Core as _Core

    logger = Logger(_Core(), None, 0, False, False, False, False, True, None, {})
    logger.add(_sys.stderr)
    _atexit.register(logger.remove)

    return logger


logger.add(
    create_logfile("{time:%Y-%m-%d}.log", "app"),
    level="INFO",
    rotation="100 MB",
    retention="30 days",
    enqueue=True,
    colorize=True,
    backtrace=True,
    diagnose=True,
    encoding="utf8",
)
logger.add(
    create_logfile("errors.log", "app"),
    level="ERROR",
    rotation="20 MB",
    enqueue=True,
    colorize=True,
    backtrace=True,
    diagnose=True,
    encoding="utf8",
)
