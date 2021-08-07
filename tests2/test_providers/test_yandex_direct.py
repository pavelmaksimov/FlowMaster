import datetime as dt


def test_reports_to_csv(ya_direct_report_to_csv_notebook):
    from flowmaster.operators.etl.service import ETLOperator

    etl_flow = ETLOperator(ya_direct_report_to_csv_notebook)
    etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2).execute()


def test_reports_to_clickhouse(ya_direct_report_to_clickhouse_notebook):
    from flowmaster.operators.etl.service import ETLOperator

    etl_flow = ETLOperator(ya_direct_report_to_clickhouse_notebook)
    etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2).execute()


def test_attributes_to_csv(ya_direct_campaigns_to_csv_notebook):
    from flowmaster.operators.etl.service import ETLOperator

    etl_flow = ETLOperator(ya_direct_campaigns_to_csv_notebook)
    etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2).execute()


def test_attributes_to_clickhouse(ya_direct_campaigns_to_clickhouse_notebook):
    from flowmaster.operators.etl.service import ETLOperator

    etl_flow = ETLOperator(ya_direct_campaigns_to_clickhouse_notebook)
    etl_flow(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1), max_pages=2).execute()
