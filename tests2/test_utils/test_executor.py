import datetime as dt

from flowmaster.operators.etl.providers.yandex_metrika_logs import (
    YandexMetrikaLogsExportPolicy,
)
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.utils.thread_executor import catch_exceptions, ThreadExecutor
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir
from tests.fixtures.yandex_metrika import yml_visits_to_file_config as CONFIG

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")
CONFIG.export.credentials = YandexMetrikaLogsExportPolicy.CredentialsPolicy(
    **credentials["yandex-metrika-logs"]
)


def test_thread_executor_yandex_metrika_logs():
    @catch_exceptions
    def order_task(*args, **kwargs):
        count_flows = 4
        worktimes = [dt.datetime(2021, 1, i + 1) for i in range(count_flows)]

        for worktime in worktimes:
            CONFIG.load.file_name = (
                f"{test_thread_executor_yandex_metrika_logs.__name__}.tsv"
            )

            flow = ETLOperator(CONFIG)
            generator = flow(start_period=worktime, end_period=worktime)

            yield generator

    flow_scheduler = ThreadExecutor(order_task_func=order_task)
    flow_scheduler.start(workers=4, orders=1)

    assert True
