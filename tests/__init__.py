from pathlib import Path

from flowmaster.database import db
from flowmaster.models import FlowItem

db.create_tables([FlowItem])


def get_tests_dir(test_dir_name: str = "tests") -> Path:
    cwd = Path.cwd()
    if "flowmaster-dev" in cwd.parts:
        parts_to_fm = cwd.parts[: cwd.parts.index("flowmaster-dev") + 1]
        return Path(*parts_to_fm) / test_dir_name
    elif "flowmaster-dev" in cwd.iterdir():
        return cwd / "flowmaster-dev" / test_dir_name
    else:
        raise FileNotFoundError
