from freezegun import freeze_time

from flowmaster.operators.etl.service import ETLOperator
from tests.fixtures.yandex_metrika import yml_visits_to_csv_notebook


@freeze_time("2021-01-01")
def test_jinja_template():
    yml_visits_to_csv_notebook.name = "flow"
    yml_visits_to_csv_notebook.load.file_name = (
        "{{name}} {{provider}} {{storage}} {{ datetime.date() }}.tsv"
    )
    yml_visits_to_csv_notebook.load.add_data_before = (
        "{{name}} {{provider}} {{storage}} {{ datetime.date() }}.tsv"
    )
    yml_visits_to_csv_notebook.load.add_data_after = (
        "{{name}} {{provider}} {{storage}} {{ datetime.date() }}.tsv"
    )

    flow = ETLOperator(yml_visits_to_csv_notebook)

    assert flow.Load.file_name == "flow yandex_metrika_logs csv 2021-01-01.tsv"
    assert flow.Load.add_data_before == "flow yandex_metrika_logs csv 2021-01-01.tsv"
    assert flow.Load.add_data_after == "flow yandex_metrika_logs csv 2021-01-01.tsv"
