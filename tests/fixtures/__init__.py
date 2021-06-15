import datetime as dt
from pathlib import Path

from flowmaster.operators.etl.loaders.csv.policy import (
    CSVLoadPolicy,
    CSVTransformPolicy,
)
from flowmaster.operators.etl.policy import ETLNotebook
from tests import get_tests_dir

FILE_TESTS_DIR = get_tests_dir() / "__test_files__"
Path.mkdir(FILE_TESTS_DIR, exist_ok=True)

work_policy = ETLNotebook.WorkPolicy(
    triggers=ETLNotebook.WorkPolicy.TriggersPolicy(
        schedule=ETLNotebook.WorkPolicy.TriggersPolicy.SchedulePolicy(
            timezone="Europe/Moscow",
            start_time="00:00:00",
            from_date=dt.date.today() - dt.timedelta(5),
            interval="daily",
        )
    )
)
csv_load_policy = CSVLoadPolicy(path=str(FILE_TESTS_DIR), save_mode="w")
csv_transform_policy = CSVTransformPolicy(error_policy="default")
