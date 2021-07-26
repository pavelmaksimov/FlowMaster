import datetime as dt
import hashlib
from typing import TYPE_CHECKING, Iterator, Union

from flowmaster.exceptions import AuthError
from flowmaster.executors import SleepIteration
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.providers.criteo.policy import (
    CriteoExportPolicy as ExportPolicy,
)

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook


class CriteoExport(ExportAbstract):
    def __init__(self, notebook: "ETLNotebook", *args, **kwargs):
        self.export: ExportPolicy = notebook.export
        self.client = Criteo(
            wait_report=False,
            processing_mode="offline",
            retry_if_not_enough_units=False,
            retry_if_exceeded_limit=False,
            retries_if_server_error=0,
            **self.export.credentials.dict(exclude_none=True),
            **self.export.headers.dict(exclude_none=True),
        )
        super(CriteoExport, self).__init__(notebook, *args, **kwargs)

    def exclude_none(self, body: dict) -> dict:
        new_body = {}
        for key, value in body.items():
            if isinstance(value, dict):
                new_body[key] = self.exclude_none(value)
            elif value is not None:
                new_body[key] = value

        return new_body

    def create_report_name(self, body):
        return hashlib.md5(str((body, self.export.headers)).encode()).hexdigest()

    def collect_params(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> dict:
        body = self.export.body.dict(exclude_none=True)
        body = self.exclude_none(body)

        if self.export.resource == "reports":
            body["params"]["Format"] = "TSV"
            body["params"]["ReportName"] = self.create_report_name(body)

            if body["params"]["DateRangeType"] == "CUSTOM_DATE":
                body["params"]["SelectionCriteria"].update(
                    {
                        "DateFrom": start_period.date().isoformat(),
                        "DateTo": end_period.date().isoformat(),
                    }
                )

        return super().collect_params(start_period, end_period, **body)

    def __call__(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> Iterator[Union[ExportContext, SleepIteration]]:
        self.logger.info("Exportation data")


            except exceptions.CriteoTokenError as exc:
                raise AuthError(exc)

            except exceptions.CriteoNotEnoughUnitsError:
                yield SleepIteration(sleep=60 * 5)
                continue

            except exceptions.CriteoRequestsLimitError:
                yield SleepIteration(sleep=10)
                continue

            except exceptions.CriteoClientError as exc:
                if api_error_retries and exc.error_code in (52, 1000, 1001, 1002):
                    api_error_retries -= 1
                    yield SleepIteration(sleep=10)
                    continue
                raise

            except ConnectionError:
                if api_error_retries:
                    api_error_retries -= 1
                    yield SleepIteration(sleep=10)
                raise

                    yield ExportContext(
                        export_kwargs=result.request_kwargs,
                        columns=result.columns,
                        data=data,
                        data_orient=DataOrient.values,
                    )

                    self.logger.info("Iter export data")
