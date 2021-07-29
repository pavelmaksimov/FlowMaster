from typing import TYPE_CHECKING, Iterator, Optional

from flowmaster.executors import catch_exceptions, ExecutorIterationTask
from flowmaster.models import FlowItem, FlowStatus
from flowmaster.operators.base.work import Work
from flowmaster.service import iter_active_notebook_filenames, get_notebook
from flowmaster.utils.logging_helper import Logger, getLogger
from flowmaster.utils.logging_helper import logger

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
        self.logger = logger or getLogger()

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
            notebook_hash=self.notebook.hash,
            max_fatal_errors=self.max_fatal_errors,
            update_stale_data=self.update_stale_data,
        )


@catch_exceptions
def ordering_etl_flow_tasks(
    *, dry_run: bool = False
) -> Iterator[ExecutorIterationTask]:
    """Prepare flow function to be sent to the queue and executed"""
    from flowmaster.operators.etl.service import ETLOperator
    from flowmaster.operators.etl.policy import ETLNotebook

    for name in iter_active_notebook_filenames():
        validate, text, notebook_dict, notebook, error = get_notebook(name)
        notebook: ETLNotebook
        if dry_run:
            if notebook.provider != "fakedata":
                continue

        if not validate:
            logger.error("ValidationError: '{}': {}", name, error)
            continue

        flow = ETLOperator(notebook)

        for start_period, end_period in flow.Work.iter_period_for_execute():
            etl_flow_task = flow(start_period, end_period, dry_run=dry_run)

            # The status is changed so that there is no repeated ordering of tasks.
            FlowItem.change_status(
                flow.name,
                new_status=FlowStatus.run,
                from_time=start_period,
                to_time=end_period,
            )
            logger.info(
                "Order ETL flow [{}]: {} {}", flow.name, start_period, end_period
            )

            yield etl_flow_task
