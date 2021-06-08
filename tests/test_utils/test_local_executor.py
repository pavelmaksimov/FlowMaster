import mock

from flowmaster.models import FlowItem
from flowmaster.utils.local_executor import sync_executor
from flowmaster.utils.yaml_helper import YamlHelper
from tests.fixtures.fakedata import fakedata_to_csv_config


def test_local_executor():
    FlowItem.clear("test_local_executor")
    config = fakedata_to_csv_config.dict()
    config.pop("name")
    YamlHelper.iter_parse_file_from_dir = mock.Mock(
        return_value=(("test_local_executor", config),)
    )

    sync_executor(orders=1, dry_run=True)

    items = list(FlowItem.iter_items("test_local_executor"))

    assert len(items) == 5
