import datetime as dt
from pathlib import Path

from flowmaster.operators.etl.config import ETLFlowConfig
from flowmaster.operators.etl.loaders.file.policy import FileLoadPolicy
from flowmaster.operators.etl.transform.policy import FileTransformPolicy
from tests import get_tests_dir

FILE_TESTS_DIR = get_tests_dir() / "__test_files__"
Path.mkdir(FILE_TESTS_DIR, exist_ok=True)

work_policy = ETLFlowConfig.Work(
    schedule=ETLFlowConfig.Work.Schedule(
        timezone="Europe/Moscow",
        start_time="00:00:00",
        from_date=dt.date.today() - dt.timedelta(5),
        interval="daily",
    )
)
file_load_policy = FileLoadPolicy(path=str(FILE_TESTS_DIR), save_mode="w")
file_transform_policy = FileTransformPolicy(error_policy="default")
