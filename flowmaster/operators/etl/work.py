from logging import Logger, getLogger
from typing import Optional, TYPE_CHECKING, Iterator

import pydantic

from flowmaster.executors import catch_exceptions, ExecutorIterationTask
from flowmaster.models import FlowItem, FlowStatus
from flowmaster.operators.base.work import Work
from flowmaster.setttings import Settings
from flowmaster.utils.yaml_helper import YamlHelper

if TYPE_CHECKING:
    from flowmaster.operators.etl.service import ETLOperator
    from flowmaster.operators.etl.policy import ETLNotebook


class ETLWork(Work):
    def __init__(self, notebook: "ETLNotebook", logger: Optional[Logger] = None):
        super(ETLWork, self).__init__(notebook, logger)

        self.update_stale_data = notebook.work.update_stale_data
        self.export_pool_names = [
            *self.concurrency_pool_names,
            *(notebook.export.pools or []),
        ]
        self.transform_pool_names = [
            *self.concurrency_pool_names,
            *(notebook.transform.pools or []),
        ]
        self.load_pool_names = [
            *self.concurrency_pool_names,
            *(notebook.load.pools or []),
        ]

        if notebook.export.concurrency is not None:
            self.export_pool_names.append(f"__{self.name}_export_concurrency__")
            self.add_pool(
                f"__{self.name}_export_concurrency__", notebook.export.concurrency
            )

        if notebook.transform.concurrency is not None:
            self.transform_pool_names.append(f"__{self.name}_transform_concurrency__")
            self.add_pool(
                f"__{self.name}_transform_concurrency__", notebook.transform.concurrency
            )

        if notebook.load.concurrency is not None:
            self.load_pool_names.append(f"__{self.name}_load_concurrency__")
            self.add_pool(
                f"__{self.name}_load_concurrency__", notebook.load.concurrency
            )

        self.Model = FlowItem
        self.logger = logger or getLogger(__name__)

    def iter_items_for_execute(self) -> Iterator[FlowItem]:
        """
        Collects all flow items for execute.
        """
        return self.Model.get_items_for_execute(
            self.name,
            self.current_worktime,
            self.start_datetime,
            self.interval_timedelta,
            self.keep_sequence,
            self.retries,
            self.retry_delay,
            notebook_hash="",
            max_fatal_errors=3,
            update_stale_data=self.update_stale_data,
        )


@catch_exceptions
def ordering_etl_flow_tasks(
    *, logger: Logger, dry_run: bool = False
) -> Iterator[ExecutorIterationTask]:
    """Prepare flow function to be sent to the queue and executed"""
    from flowmaster.operators.etl.service import ETLOperator
    from flowmaster.operators.etl.policy import ETLNotebook

    for file_name, notebook_dict in YamlHelper.iter_parse_file_from_dir(
        Settings.NOTEBOOKS_DIR, match=".etl.flow"
    ):
        if dry_run:
            if notebook_dict.get("provider") != "fakedata":
                continue

        try:
            notebook = ETLNotebook(name=file_name, **notebook_dict)
        except pydantic.ValidationError as exc:
            logger.error("ValidationError: '%s': %s", file_name, exc)
            continue
        except Exception as exc:
            logger.error("Error: '%s': %s", file_name, exc)
            continue

        work = ETLWork(notebook)

        for start_period, end_period in work.iter_period_for_execute():
            etl_flow = ETLOperator(notebook)
            etl_flow_task = etl_flow(start_period, end_period, dry_run=dry_run)

            # The status is changed so that there is no repeated ordering of tasks.
            FlowItem.change_status(
                etl_flow.name,
                new_status=FlowStatus.run,
                from_time=start_period,
                to_time=end_period,
            )
            logger.info(
                "Order ETL flow [%s]: %s %s", etl_flow.name, start_period, end_period
            )

            yield etl_flow_task
