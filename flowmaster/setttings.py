import os
import sys
from pathlib import Path


class Settings:
    if APP_HOME := os.environ.get("FLOWMASTER_HOME"):
        APP_HOME = Path(APP_HOME)
    else:
        APP_HOME = Path.home() / "FlowMaster"

    FILE_STORAGE_DIR = APP_HOME / "storage"
    NOTEBOOKS_DIR = APP_HOME / "notebooks"
    LOGS_DIR = APP_HOME / "logs"
    PLUGINS_DIRNAME = "plugins"
    PLUGINS_DIR = APP_HOME / PLUGINS_DIRNAME
    POOL_CONFIG_FILEPATH = APP_HOME / "pools.yaml"


# For import plugins.
sys.path.append(str(Settings.APP_HOME))
