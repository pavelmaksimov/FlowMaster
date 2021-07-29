import datetime as dt

from flowmaster.operators.etl.service import ETLOperator
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")
criteo_credentials = credentials["criteo"]


def test(criteo_to_csv_notebook):
    criteo_to_csv_notebook.export.credentials.client_id = credentials["criteo"]["client_id"]
    criteo_to_csv_notebook.export.credentials.client_secret = credentials["criteo"]["client_secret"]
    flow = ETLOperator(criteo_to_csv_notebook)
    list(flow(dt.datetime(2021, 7, 27), dt.datetime(2021, 7, 27)))
    with flow.Load.open_file() as file:
        assert file.read() == 'Day\tClicks\n"2021-07-27"\t"1352"\n'
