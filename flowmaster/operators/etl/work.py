from typing import TYPE_CHECKING, Iterator, Optional

from flowmaster.executors import catch_exceptions, ExecutorIterationTask
from flowmaster.models import FlowItem
from flowmaster.operators.base.work import Work, prepare_items_for_order
from flowmaster.service import iter_active_notebook_filenames, get_notebook
from flowmaster.utils.logging_helper import Logger, getLogger
from flowmaster.utils.logging_helper import logger

if TYPE_CHECKING:
    from flowmaster.operators.etl.core import ETLOperator
    from flowmaster.operators.etl.policy import ETLNotebook


class ETLWork(Work):
    def __init__(self, notebook: "ETLNotebook", logger: Optional[Logger] = None):
        super(ETLWork, self).__init__(notebook, logger)

        self.update_stale_data = notebook.work.update_stale_data

        self.Model = FlowItem
        self.logger = logger or getLogger()

    def iter_items_for_execute(self) -> Iterator[FlowItem]:
        """
        Collects all flow items for execute.
        """
        # TODO: Переименовать update_stale_data в update_range, этот параметр не только для ETL,
        #  он и для других операторов нужен, поэтому удалить эту и завернуть все в одну функцию.
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
    # TODO: избавиться от функции, переделать так, чтобы одна функция была для заказа

    from flowmaster.operators.etl.core import ETLOperator
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
            etl_flow_task = flow.task(start_period, end_period, dry_run=dry_run)

            with prepare_items_for_order(flow, start_period, end_period):
                logger.info(
                    "Order ETL flow [{}]: {} {}", flow.name, start_period, end_period
                )
                yield etl_flow_task
