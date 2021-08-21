import datetime as dt


def test_management_clients(ymm_clients_to_csv_notebook):
    from flowmaster.flow import Flow

    flow = Flow(ymm_clients_to_csv_notebook)
    flow.dry_run(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1))


def test_management_goals(ymm_goals_to_csv_notebook):
    from flowmaster.flow import Flow

    flow = Flow(ymm_goals_to_csv_notebook)
    flow.dry_run(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1))


def test_management_counters(ymm_counters_to_csv_notebook):
    from flowmaster.flow import Flow

    flow = Flow(ymm_counters_to_csv_notebook)
    flow.dry_run(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1))


def test_stats(ymstats_to_csv_notebook):
    from flowmaster.flow import Flow

    flow = Flow(ymstats_to_csv_notebook)
    flow.dry_run(dt.datetime(2021, 2, 1), dt.datetime(2021, 2, 1))


def test_logs(ya_metrika_logs_to_csv_notebook2):
    from flowmaster.flow import Flow

    flow = Flow(ya_metrika_logs_to_csv_notebook2)
    flow.dry_run(dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 1))
