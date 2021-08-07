from freezegun import freeze_time

from flowmaster.operators.etl.core import ETLOperator


@freeze_time("2021-01-01")
def test_jinja_template(ya_metrika_logs_to_csv_notebook):
    ya_metrika_logs_to_csv_notebook.name = "flow"
    ya_metrika_logs_to_csv_notebook.load.file_name = (
        "{{name}} {{provider}} {{storage}} {{ datetime.date() }}.tsv"
    )
    ya_metrika_logs_to_csv_notebook.load.add_data_before = (
        "{{name}} {{provider}} {{storage}} {{ datetime.date() }}.tsv"
    )
    ya_metrika_logs_to_csv_notebook.load.add_data_after = (
        "{{name}} {{provider}} {{storage}} {{ datetime.date() }}.tsv"
    )

    flow = ETLOperator(ya_metrika_logs_to_csv_notebook)

    assert flow.Load.file_name == "flow yandex_metrika_logs csv 2021-01-01.tsv"
    assert flow.Load.add_data_before == "flow yandex_metrika_logs csv 2021-01-01.tsv"
    assert flow.Load.add_data_after == "flow yandex_metrika_logs csv 2021-01-01.tsv"
