import datetime as dt
import time
from typing import Iterator, Union, Optional

from flowmaster.exceptions import FatalError
from flowmaster.executors import (
    SleepIteration,
    NextIterationInPools,
    AsyncIterationT,
    ExecutorIterationTask,
)
from flowmaster.models import FlowStatus, FlowETLStep, FlowOperator
from flowmaster.operators.base.core import BaseOperator
from flowmaster.operators.etl.dataschema import ETLContext
from flowmaster.operators.etl.loaders import storage_classes
from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.operators.etl.providers import provider_classes
from flowmaster.operators.etl.work import ETLWork
from flowmaster.utils import iter_range_datetime
from flowmaster.utils.logging_helper import create_logfile


class ETLOperator(BaseOperator):
    items = None

    def __init__(self, notebook: ETLNotebook):
        super(ETLOperator, self).__init__(notebook)
        self.notebook: ETLNotebook

        provider_meta_class = provider_classes[notebook.provider]
        load_class = storage_classes[notebook.storage]

        self.Work = ETLWork(notebook, self.logger)
        self.Provider = provider_meta_class(notebook, self.logger)
        self.Load = load_class(notebook, self.logger)

        self.operator_context = ETLContext(storage=notebook.storage)

    def iterator(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> Iterator[Union[dict, AsyncIterationT]]:

        begin_time = time.time()

        try:
            yield {
                self.Model.status.name: FlowStatus.run,
                self.Model.started_utc.name: dt.datetime.utcnow(),
                self.Model.data.name: self.operator_context.dict(exclude_unset=True),
            }

            with self.Load as load:
                export_iterator = self.Provider.Export(
                    start_period, end_period, **kwargs
                )

                while True:
                    # Export step.
                    yield {self.Model.etl_step.name: FlowETLStep.export}
                    yield NextIterationInPools(pool_names=self.Work.export_pool_names)
                    try:
                        result = next(export_iterator)

                        if isinstance(result, SleepIteration):
                            sleep_task = result
                            yield sleep_task
                            continue

                    except StopIteration:
                        # Successful completion of the flow.
                        break

                    # Transform step.
                    self.operator_context.export_kwargs.update(
                        {**kwargs, **result.export_kwargs}
                    )
                    yield {
                        self.Model.etl_step.name: FlowETLStep.transform,
                        self.Model.data.name: self.operator_context.dict(
                            exclude_unset=True
                        ),
                    }
                    yield NextIterationInPools(
                        pool_names=self.Work.transform_pool_names
                    )
                    transform_context = self.Provider.Transform(
                        result,
                        storage_data_orient=self.Load.data_orient,
                    )

                    # Load step.
                    self.operator_context.size += transform_context.size
                    self.operator_context.number_rows += len(transform_context.data)
                    self.operator_context.number_error_lines += len(
                        transform_context.data_errors
                    )
                    yield {
                        self.Model.etl_step.name: FlowETLStep.load,
                        self.Model.data.name: self.operator_context.dict(
                            exclude_unset=True
                        ),
                        "data_errors": transform_context.data_errors,
                    }
                    yield NextIterationInPools(pool_names=self.Work.load_pool_names)
                    load(transform_context)

        except FatalError as er:
            yield {
                self.Model.status.name: FlowStatus.fatal_error,
                self.Model.log.name: str(er),
            }
            raise

        except Exception as er:
            yield {
                self.Model.status.name: FlowStatus.error,
                self.Model.log.name: str(er),
            }
            raise

        except:
            yield {
                self.Model.status.name: FlowStatus.error,
                self.Model.log.name: "Unknown error",
            }
            raise

        else:
            self.operator_context.export_kwargs.clear()
            yield {
                self.Model.etl_step.name: None,
                self.Model.status.name: FlowStatus.success,
                self.Model.retries.name: 0,
                self.Model.data.name: self.operator_context.dict(exclude_unset=True),
            }

        finally:
            yield {
                self.Model.finished_utc.name: dt.datetime.utcnow(),
                self.Model.duration.name: round((time.time() - begin_time) / 60) or 1,
            }

    @staticmethod
    def _get_period_text(start_period: dt.datetime, end_period: dt.datetime) -> str:
        if start_period == end_period:
            return start_period.strftime("%Y-%m-%dT%H-%M-%S").replace("T00-00-00", "")
        else:
            return "{} {}".format(
                start_period.strftime("%Y-%m-%dT%H-%M-%S"),
                end_period.strftime("%Y-%m-%dT%H-%M-%S"),
            ).replace("T00-00-00", "")

    def get_logfile_path(self):
        period_text = self._get_period_text(
            self.operator_context.start_period, self.operator_context.end_period
        )
        return create_logfile(f"{period_text}.log", self.name)

    def task_generator(
        self,
        start_period: dt.datetime,
        end_period: dt.datetime,
        *,
        dry_run: bool = False,
        **kwargs,
    ) -> Iterator[Union[dict, Optional[AsyncIterationT]]]:

        self.operator_context.start_period = start_period
        self.operator_context.end_period = end_period
        self.Load.update_context(self.operator_context)
        self.add_logger_file(dry_run)

        period_text = self._get_period_text(start_period, end_period)
        datetime_list = iter_range_datetime(
            start_period, end_period, self.Work.interval_timedelta
        )
        self.items = self.Model.create_items(
            self.name,
            datetime_list,
            **{
                self.Model.operator.name: FlowOperator.etl,
                self.Model.data.name: self.operator_context.dict(exclude_unset=True),
                self.Model.logpath.name: str(self.get_logfile_path().absolute()),
            },
        )

        log_data = {}
        try:
            self.logger.info("Start flow: {} {}", self.name, period_text)

            for item in self.iterator(start_period, end_period, **kwargs):
                if isinstance(item, dict):
                    log_data = item
                    self.Model.update_items(self.items, **log_data)
                    self.logger.info("{}: {}", self.name, log_data)

                yield item

        except:
            self.logger.exception("Fail flow: {}  {}", self.name, period_text)
            if dry_run is False:
                self.send_notifications(
                    **{
                        "period": self._get_period_text(start_period, end_period),
                        **log_data,
                    }
                )
            raise

        else:
            if dry_run is False:
                self.send_notifications(
                    FlowStatus.success,
                    period=self._get_period_text(start_period, end_period),
                )

        finally:
            self.Model.update_items(self.items)
            self.logger.info("End flow: {}  {}", self.name, period_text)

    def __call__(
        self,
        start_period: dt.datetime,
        end_period: dt.datetime,
        *,
        dry_run: bool = False,
        **kwargs,
    ) -> ExecutorIterationTask:
        # TODO: перенести ExecutorIterationTask в BaseOperator,
        #  чтобы не добавлять его везде и как то через маг.классы итерации спрятать
        return ExecutorIterationTask(
            self.task_generator(start_period, end_period, dry_run=dry_run, **kwargs),
            expires=self.Work.expires,
            soft_time_limit_seconds=self.Work.soft_time_limit_seconds,
        )
