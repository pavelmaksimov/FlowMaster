from pathlib import Path

from flowmaster.operators.etl.loaders.csv.service import CSVLoader
from flowmaster.operators.etl.policy import ETLFlowConfig
from flowmaster.operators.etl.providers import FakeDataProvider
from flowmaster.operators.etl.providers.fakedata import FakeDataExportPolicy
from tests import get_tests_dir
from tests.fixtures import work_policy, csv_load_policy, csv_transform_policy

FILE_TESTS_DIR = get_tests_dir() / "__test_files__"
Path.mkdir(FILE_TESTS_DIR, exist_ok=True)

fakedata_to_csv_config = ETLFlowConfig(
    name="fakedata_to_csv_config",
    provider=FakeDataProvider.name,
    storage=CSVLoader.name,
    work=work_policy,
    export=FakeDataExportPolicy(rows=1),
    transform=csv_transform_policy,
    load=csv_load_policy,
)
