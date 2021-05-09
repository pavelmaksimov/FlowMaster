import datetime as dt
import hashlib
from typing import TYPE_CHECKING, Iterator

from tapi_yandex_direct import YandexDirect
from tapi_yandex_direct import exceptions

from flowmaster.exceptions import AuthError
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.providers.yandex_direct.policy import (
    YandexDirectExportPolicy as ExportPolicy,
)
from flowmaster.operators.etl.types import DataOrient
from flowmaster.utils.executor import SleepTask

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig


class YandexDirectExport(ExportAbstract):
    def __init__(self, config: "ETLFlowConfig", *args, **kwargs):
        self.export: ExportPolicy = config.export
        self.credentials = self.export.credentials
        self.resource = self.export.resource
        self.body: ExportPolicy.BodyPolicy = self.export.body
        self.headers: ExportPolicy.HeadersPolicy = self.export.headers
        self.client = YandexDirect(
            wait_report=False,
            processing_mode="offline",
            retry_if_not_enough_units=False,
            retry_if_exceeded_limit=False,
            retries_if_server_error=0,
            **self.credentials.dict(exclude_none=True),
            **self.headers.dict(exclude_none=True),
        )
        super(YandexDirectExport, self).__init__(config, *args, **kwargs)

    @classmethod
    def validate_params(cls, **params: dict) -> None:
        pass

    def exclude_none(self, body: dict) -> dict:
        new_body = {}
        for key, value in body.items():
            if isinstance(value, dict):
                new_body[key] = self.exclude_none(value)
            elif value is not None:
                new_body[key] = value

        return new_body

    def create_report_name(self, body):
        report_params = str((body, self.headers))
        return hashlib.md5(str(report_params).encode()).hexdigest()

    def collect_params(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> dict:
        body = self.body.dict(exclude_none=True)
        body = self.exclude_none(body)

        if self.resource == "reports":
            body["params"]["ReportName"] = self.create_report_name(body)

            if body["params"]["DateRangeType"] == "CUSTOM_DATE":
                body["params"]["SelectionCriteria"][
                    "DateFrom"
                ] = start_period.date().isoformat()

                body["params"]["SelectionCriteria"][
                    "DateTo"
                ] = end_period.date().isoformat()

        return super().collect_params(start_period, end_period, **body)

    def __call__(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> Iterator[ExportContext]:
        self.logger.info("Exportation data")
        body = self.collect_params(start_period, end_period)
        method = getattr(self.client, self.resource)

        result = None
        page_iterator = None
        api_error_retries = 10
        while api_error_retries:
            try:
                if result is None:
                    result = method().post(data=body)

                if self.resource != "reports":

                    if page_iterator is None:
                        page_iterator = result().pages(max_pages=kwargs.get("max_pages"))

                    page = next(page_iterator)

            except exceptions.YandexDirectTokenError as exc:
                raise AuthError(exc)

            except exceptions.YandexDirectNotEnoughUnitsError:
                yield SleepTask(sleep=60 * 5)
                continue

            except exceptions.YandexDirectRequestsLimitError:
                yield SleepTask(sleep=10)
                continue

            except exceptions.YandexDirectClientError as exc:
                if exc.error_code in (52, 1000, 1001, 1002):
                    api_error_retries -= 1
                    yield SleepTask(sleep=10)
                    continue
                raise

            except ConnectionError:
                api_error_retries -= 1
                yield SleepTask(sleep=10)

            except StopIteration:
                break

            else:
                if self.resource == "reports":
                    if result.status_code in (201, 202):
                        result = None
                        yield SleepTask(sleep=10)
                        continue

                    data = result().to_values()

                    yield ExportContext(
                        export_kwargs=result.request_kwargs,
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
                        export_kwargs=page.request_kwargs,
                        columns=columns,
                        data=page.data,
                        data_orient=DataOrient.dict,
                    )
