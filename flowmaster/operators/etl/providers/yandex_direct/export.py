import datetime as dt
import hashlib
from typing import TYPE_CHECKING, Iterator, Union

from flowmaster.exceptions import AuthError
from flowmaster.executors import SleepIteration
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.providers.yandex_direct.policy import (
    YandexDirectExportPolicy as ExportPolicy,
)

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook


class YandexDirectExport(ExportAbstract):
    def __init__(self, notebook: "ETLNotebook", *args, **kwargs):
        from tapi_yandex_direct import YandexDirect

        self.export: ExportPolicy = notebook.export
        self.client = YandexDirect(
            wait_report=False,
            processing_mode="offline",
            retry_if_not_enough_units=False,
            retry_if_exceeded_limit=False,
            retries_if_server_error=0,
            **self.export.credentials.dict(exclude_none=True),
            **self.export.headers.dict(exclude_none=True),
        )
        super(YandexDirectExport, self).__init__(notebook, *args, **kwargs)

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
        from tapi_yandex_direct import exceptions

        self.logger.info("Exportation data")

        result = None
        page_iterator = None
        api_error_retries = 10
        while True:
            try:
                if result is None:
                    body = self.collect_params(start_period, end_period)
                    method = getattr(self.client, self.export.resource)
                    result = method().post(data=body)

                if self.export.resource != "reports":

                    if page_iterator is None:
                        page_iterator = result().pages(
                            max_pages=kwargs.get("max_pages")
                        )

                    page = next(page_iterator)

            except exceptions.YandexDirectTokenError as exc:
                raise AuthError(exc)

            except exceptions.YandexDirectNotEnoughUnitsError:
                yield SleepIteration(sleep=60 * 5)
                continue

            except exceptions.YandexDirectRequestsLimitError:
                yield SleepIteration(sleep=10)
                continue

            except exceptions.YandexDirectClientError as exc:
                if api_error_retries and exc.error_code in (52, 1000, 1001, 1002):
                    api_error_retries -= 1
                    yield SleepIteration(sleep=10)
                    continue
                raise

            except ConnectionError:
                if api_error_retries:
                    api_error_retries -= 1
                    yield SleepIteration(sleep=10)
                    continue
                raise

            except StopIteration:
                break

            else:
                if self.export.resource == "reports":
                    if result.status_code in (201, 202):
                        result = None
                        yield SleepIteration(sleep=10)
                        continue

                    data = result().to_values()

                    yield ExportContext(
                        request_kwargs=result.request_kwargs,
                        columns=result.columns,
                        data=data,
                        data_orient=DataOrient.values,
                    )

                    break
                else:
                    self.logger.info("Iter export data")

                    columns = []
                    if page.data:
                        columns = sorted(page.data[0].keys())

                    yield ExportContext(
                        request_kwargs=page.request_kwargs,
                        columns=columns,
                        data=page.data,
                        data_orient=DataOrient.dict,
                    )
