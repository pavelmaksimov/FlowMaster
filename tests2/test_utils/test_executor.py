import pendulum
from tests.fixtures.yandex_metrika import ya_metrika_logs_to_csv_notebook as NOTEBOOK

from flowmaster.executors import (
    catch_exceptions,
    ExecutorIterationTask,
    ThreadAsyncExecutor,
)
from flowmaster.operators.etl.providers.yandex_metrika_logs import (
    YandexMetrikaLogsExportPolicy,
)
from flowmaster.operators.etl.service import ETLOperator
from flowmaster.utils.yaml_helper import YamlHelper
from tests import get_tests_dir

credentials = YamlHelper.parse_file(get_tests_dir("tests2") / "credentials.yml")
NOTEBOOK.export.credentials = YandexMetrikaLogsExportPolicy.CredentialsPolicy(
    **credentials["yandex-metrika-logs"]
)


def test_thread_executor_yandex_metrika_logs():
    @catch_exceptions
    def order_task(*args, **kwargs):
        count_flows = 4
        worktimes = [pendulum.datetime(2021, 1, i + 1) for i in range(count_flows)]

        for worktime in worktimes:
            NOTEBOOK.load.file_name = (
                f"{test_thread_executor_yandex_metrika_logs.__name__}.tsv"
            )

            flow = ETLOperator(NOTEBOOK)
            generator = flow(start_period=worktime, end_period=worktime)

            yield ExecutorIterationTask(generator)

    flow_scheduler = ThreadAsyncExecutor(ordering_task_func=order_task)
    flow_scheduler.start(workers=4, orders=1)

    assert True
