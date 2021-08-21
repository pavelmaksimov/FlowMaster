import datetime as dt
import time
from typing import Iterator, Union, Optional

import pendulum

from flowmaster.enums import Statuses, Operators
from flowmaster.exceptions import FatalError
from flowmaster.executors import (
    SleepIteration,
    NextIterationInPools,
    AsyncIterationT,
)
from flowmaster.operators.base.core import BaseOperator
from flowmaster.operators.etl.dataschema import ETLContext
from flowmaster.operators.etl.enums import ETLSteps
from flowmaster.operators.etl.loaders import Loaders
from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.operators.etl.providers import Providers
from flowmaster.operators.etl.work import ETLWork
from flowmaster.utils import iter_range_datetime
from flowmaster.utils.logging_helper import create_logfile


class ETLOperator(BaseOperator):
    name = "etl"
    work_class = ETLWork
    items = None
    Providers = Providers
    Loaders = Loaders

    def __init__(self, notebook: ETLNotebook):
        super(ETLOperator, self).__init__(notebook)
        self.notebook: ETLNotebook

        self.Provider = Providers(notebook, self.logger)
        self.Load = Loaders(notebook, self.logger)

        self.operator_context = ETLContext(
            operator=Operators.etl, storage=notebook.storage
        )

        # Adding pools.
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

    def __call__(
        self,
        start_period: dt.datetime,
        end_period: dt.datetime,
        *,
        dry_run: bool = False,
        **kwargs,
    ) -> Iterator[Union[dict, AsyncIterationT]]:

        begin_time = time.time()
        try:
            yield {
                self.Model.status.name: Statuses.run,
                self.Model.started_utc.name: pendulum.now("UTC"),
                self.Model.data.name: self.operator_context.dict(exclude_unset=True),
            }
            with self.Load as load:
                export_iterator = self.Provider.Export(
                    start_period, end_period, **kwargs
                )
                while True:
                    # Export step.
                    self.operator_context.step = ETLSteps.export
                    yield {
                        self.Model.data.name: self.operator_context.dict(
                            exclude_unset=True
                        )
                    }
                    yield NextIterationInPools(pool_names=self.export_pool_names)
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
                    self.operator_context.step = ETLSteps.transform
                    self.operator_context.export_kwargs.update(
                        {**kwargs, **result.request_kwargs}
                    )
                    yield {
                        self.Model.data.name: self.operator_context.dict(
                            exclude_unset=True
                        ),
                    }
                    yield NextIterationInPools(
                        pool_names=self.transform_pool_names
                    )
                    transform_context = self.Provider.Transform(result)

                    # Load step.
                    self.operator_context.step = ETLSteps.load
                    self.operator_context.size += transform_context.size
                    self.operator_context.number_rows += len(transform_context.data)
                    self.operator_context.number_error_lines += len(
                        transform_context.data_errors
                    )
                    yield {
                        self.Model.data.name: self.operator_context.dict(
                            exclude_unset=True
                        ),
                        "data_errors": transform_context.data_errors,
                    }
                    yield NextIterationInPools(pool_names=self.load_pool_names)
                    load(transform_context)

        except FatalError as er:
            yield {
                self.Model.status.name: Statuses.fatal_error,
                self.Model.info.name: str(er),
            }
            raise

        except Exception as er:
            yield {
                self.Model.status.name: Statuses.error,
                self.Model.info.name: str(er),
            }
            raise

        except:
            yield {
                self.Model.status.name: Statuses.error,
                self.Model.info.name: "Unknown error",
            }
            raise

        else:
            self.operator_context.step = None
            self.operator_context.export_kwargs.clear()
            yield {
                self.Model.status.name: Statuses.success,
                self.Model.retries.name: 0,
                self.Model.data.name: self.operator_context.dict(exclude_unset=True),
            }

        finally:
            yield {
                self.Model.finished_utc.name: pendulum.now("UTC"),
                self.Model.duration.name: round(time.time() - begin_time) or 1,
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
        return create_logfile(f"{period_text}.log", self.notebook.name)

    def _iterator(
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

        # TODO: Записи уже должны быть, их не надо создавать
        self.items = self.Model.create_items(
            self.notebook.name,
            datetime_list,
            **{
                self.Model.data.name: self.operator_context.dict(exclude_unset=True),
                self.Model.logpath.name: str(self.get_logfile_path().absolute()),
            },
        )

        log_data = {}
        try:
            self.logger.info("Start flow: {} {}", self.notebook.name, period_text)

            for item in self(start_period, end_period, dry_run=dry_run, **kwargs):
                if isinstance(item, dict):
                    log_data = item
                    self.Model.update_items(self.items, **log_data)
                    self.logger.info("{}: {}", self.notebook.name, log_data)

                yield item

        except:
            self.logger.exception("Fail flow: {}  {}", self.notebook.name, period_text)
            if dry_run is False:
                self.send_notifications(
                    **{
                        "status": Statuses.error,
                        "period": self._get_period_text(start_period, end_period),
                        **log_data,
                    }
                )
            raise

        else:
            if dry_run is False:
                self.send_notifications(
                    Statuses.success,
                    period=self._get_period_text(start_period, end_period),
                )

        finally:
            self.Model.update_items(self.items)
            self.logger.info("End flow: {}  {}", self.notebook.name, period_text)

    def dry_run(
        self,
        start_period: dt.datetime,
        end_period: dt.datetime,
        **kwargs,
    ) -> None:
        return super(ETLOperator, self).dry_run(
            start_period,
            end_period,
            **kwargs,
        )
