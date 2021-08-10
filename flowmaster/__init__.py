__author__ = "Pavel Maksimov"
__email__ = "vur21@ya.ru"
__version__ = "0.6.1"

import pathlib


def create_initial_dirs_and_files() -> None:
    from flowmaster.setttings import Settings

    pathlib.Path.mkdir(Settings.APP_HOME, exist_ok=True)
    pathlib.Path.mkdir(Settings.FILE_STORAGE_DIR, exist_ok=True)
    pathlib.Path.mkdir(Settings.LOGS_DIR, exist_ok=True)
    pathlib.Path.mkdir(Settings.NOTEBOOKS_DIR, exist_ok=True)
    pathlib.Path.mkdir(Settings.ARCHIVE_NOTEBOOKS_DIR, exist_ok=True)
    pathlib.Path.mkdir(Settings.PLUGINS_DIR, exist_ok=True)

    if not pathlib.Path.exists(Settings.POOL_CONFIG_FILEPATH):
        with open(Settings.POOL_CONFIG_FILEPATH, "w") as f:
            f.write("flows: 100\n")
