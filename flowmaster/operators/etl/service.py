import datetime as dt
import time
from typing import Iterator, Union

from flowmaster.exceptions import FatalError
from flowmaster.models import FlowStatus, FlowETLStep, FlowOperator
from flowmaster.operators.base.service import BaseOperator
from flowmaster.operators.etl.config import ETLFlowConfig
from flowmaster.operators.etl.dataschema import ETLContext
from flowmaster.operators.etl.loaders import storage_classes
from flowmaster.operators.etl.providers import provider_classes
from flowmaster.operators.etl.work import ETLWork
from flowmaster.utils import iter_range_datetime
from flowmaster.utils.executor import SleepTask, TaskPool


class ETLOperator(BaseOperator):
    items = None

    def __init__(self, config: ETLFlowConfig, *args, **kwargs):
        super(ETLOperator, self).__init__(config, *args, **kwargs)

        provider_meta_class = provider_classes[config.provider]
        load_class = storage_classes[config.storage]

        self.Work = ETLWork(config, self.logger)
        self.Provider = provider_meta_class(config, self.logger)
        self.Load = load_class(config, self.logger)

        self.operator_context = ETLContext(
            storage=self.config.storage, provider=self.config.provider
        )

    def iterator(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> Iterator[Union[dict, TaskPool, SleepTask]]:
        begin_time = time.time()
        self.operator_context.start_period = start_period
        self.operator_context.end_period = end_period

        try:
            yield {
                self.Model.status.name: FlowStatus.run,
                self.Model.started_utc.name: dt.datetime.utcnow(),
                self.Model.data.name: dict(self.operator_context),
            }

            with self.Load as load:
                export_iterator = self.Provider.Export(
                    start_period, end_period, **kwargs
                )

                while True:
                    # Export step.
                    yield {self.Model.etl_step.name: FlowETLStep.export}
                    yield TaskPool(pool_names=self.Work.export_pool_names)
                    try:
                        result = next(export_iterator)

                        if isinstance(result, SleepTask):
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
                        self.Model.data.name: dict(self.operator_context),
                    }
                    yield TaskPool(pool_names=self.Work.transform_pool_names)
                    transform_context = self.Provider.Transform(
                        result, storage_data_orient=self.Load.data_orient,
                    )

                    # Load step.
                    self.operator_context.size += transform_context.size
                    self.operator_context.number_rows += len(transform_context.data)
                    self.operator_context.number_error_lines += len(
                        transform_context.data_errors
                    )
                    yield {
                        self.Model.etl_step.name: FlowETLStep.load,
                        self.Model.data.name: dict(self.operator_context),
                        "data_errors": transform_context.data_errors,
                    }
                    yield TaskPool(pool_names=self.Work.load_pool_names)
                    load(
                        transform_context.data,
                        transform_context.insert_columns,
                        transform_context.partitions,
                    )

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
                self.Model.data.name: dict(self.operator_context),
            }

        finally:
            yield {
                self.Model.finished_utc.name: dt.datetime.utcnow(),
                self.Model.duration.name: round((time.time() - begin_time) / 60) or 1,
            }

    @staticmethod
    def _get_period_text(start_period: dt.datetime, end_period: dt.datetime) -> str:
        if start_period == end_period:
            return start_period.strftime("%Y-%m-%dT%H:%M:%S").replace("T00:00:00", "")
        else:
            return "{} {}".format(
                start_period.strftime("%Y-%m-%dT%H:%M:%S"),
                end_period.strftime("%Y-%m-%dT%H:%M:%S"),
            ).replace("T00:00:00", "")

    def __call__(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> Iterator:
        date_log = self._get_period_text(start_period, end_period)
        self.logger.update(self.name, filename=f"{date_log}.log", level=self.loglevel)
        self.logger.info("Start flow: %s  %s", self.name, date_log)

        datetime_list = iter_range_datetime(
            start_period, end_period, self.Work.interval_timedelta
        )

        self.items = self.Model.create_items(
            self.name,
            datetime_list,
            operator=FlowOperator.etl,
            data=dict(self.operator_context),
        )

        log_data = {}
        try:
            for log_data in self.iterator(start_period, end_period, **kwargs):
                if isinstance(log_data, dict):
                    self.Model.update_items(self.items, **log_data)
                    self.logger.debug("%s: %s", self.name, log_data)

                yield log_data

        except:
            self.logger.exception("Fail flow: %s  %s", self.name, date_log)
            self.send_notifications(
                **{
                    "status": FlowStatus.error,
                    "period": self._get_period_text(start_period, end_period),
                    **log_data,
                }
            )
            raise

        else:
            self.send_notifications(
                FlowStatus.success,
                period=self._get_period_text(start_period, end_period),
            )
        finally:
            self.Model.update_items(self.items)
            self.logger.info("End flow: %s  %s", self.name, date_log)
