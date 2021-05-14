from pathlib import Path

from flowmaster.operators.etl.loaders.file.service import FileLoad
from flowmaster.operators.etl.policy import ETLFlowConfig
from flowmaster.operators.etl.providers import FakeDataProvider
from flowmaster.operators.etl.providers.fakedata import FakeDataExportPolicy
from tests import get_tests_dir
from tests.fixtures import work_policy, file_load_policy, file_transform_policy

FILE_TESTS_DIR = get_tests_dir() / "__test_files__"
Path.mkdir(FILE_TESTS_DIR, exist_ok=True)

fakedata_to_file_config = ETLFlowConfig(
    name="fakedata_to_file_config",
    provider=FakeDataProvider.name,
    storage=FileLoad.name,
    work=work_policy,
    export=FakeDataExportPolicy(rows=1),
    transform=file_transform_policy,
    load=file_load_policy,
)
