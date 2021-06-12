import logging
from pathlib import Path

from flowmaster.setttings import Settings

_log_format = (
    f"%(asctime)s [%(levelname)s] %(name)s.%(funcName)s:%(lineno)d  %(message)s"
)


def get_file_handler(
    level: int, filename: str, relative_path: str = None
) -> logging.Handler:
    Path.mkdir(Settings.LOGS_DIR, exist_ok=True)
    path = Settings.LOGS_DIR

    if relative_path is not None:
        path = Settings.LOGS_DIR / relative_path
        Path.mkdir(path, exist_ok=True)

    full_path = path / filename

    file_handler = logging.FileHandler(full_path, mode="a")
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler


def get_stream_handler(level: int = logging.INFO) -> logging.Handler:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(logging.Formatter(_log_format))
    return stream_handler


def getLogger(
    name: str, filename: str = None, level: int = logging.INFO
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level - 5)
    logger.addHandler(get_stream_handler(level - 4))
    logger.addHandler(get_file_handler(logging.ERROR, filename="errors.log"))
    if filename:
        logger.addHandler(
            get_file_handler(logging.INFO, relative_path=name, filename=filename)
        )
        logger.addHandler(
            get_file_handler(logging.ERROR, relative_path=name, filename="errors.log")
        )
    return logger


class CreateLogger:
    _logger = None

    def __init__(self, name: str, filename: str = None, level: int = logging.INFO):
        self.update(name, filename, level)

    def update(self, name: str, filename: str = None, level: int = None):
        if level is None:
            level = self._logger.level
        if self._logger is not None:
            self._logger.handlers.clear()
        self._logger = getLogger(name, filename, level)

    def __getattr__(self, item):
        return getattr(self._logger, item)
