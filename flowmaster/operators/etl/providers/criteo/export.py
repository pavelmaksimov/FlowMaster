import datetime as dt
from typing import TYPE_CHECKING, Iterator, Union

import criteo_marketing_transition as cm
import orjson
from criteo_marketing_transition.rest import ApiException
from urllib3.response import HTTPResponse

from flowmaster.exceptions import AuthError, ForbiddenError
from flowmaster.executors import SleepIteration
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.providers.criteo.policy import (
    CriteoExportPolicy as ExportPolicy,
)
from flowmaster.utils import chunker

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook


class CriteoExport(ExportAbstract):
    def __init__(self, notebook: "ETLNotebook", *args, **kwargs):
        self.export: ExportPolicy = notebook.export
        configuration = cm.Configuration(
            username=self.export.credentials.client_id,
            password=self.export.credentials.client_secret,
        )

        self.client = cm.ApiClient(configuration)
        self.analytics_api = cm.AnalyticsApi(self.client)
        super(CriteoExport, self).__init__(notebook, *args, **kwargs)

    def __call__(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> Iterator[Union[ExportContext, SleepIteration]]:
        from flowmaster.operators.etl import DataOrient

        self.logger.info("Exportation data")

        columns = self.export.params.dimensions + self.export.params.metrics
        stats_query_message = cm.StatisticsReportQueryMessage(
            dimensions=self.export.params.dimensions,
            metrics=self.export.params.metrics,
            start_date=start_period.date().isoformat(),
            end_date=end_period.date().isoformat(),
            currency=self.export.params.currency,
            format="json",
        )

        iter_num = 0
        api_error_retries = 0
        while True:
            self.logger.info("Iter export data")
            iter_num += 1
            try:
                (
                    response_content,
                    http_code,
                    response_headers,
                ) = self.analytics_api.get_adset_report_with_http_info(
                    statistics_report_query_message=stats_query_message,
                    async_req=True,
                    _preload_content=False,
                ).get()

            except ApiException as exc:
                if (
                    exc.status == 401
                    or exc.body.get("error") == "credentials_no_longer_supported"
                ):
                    raise AuthError(exc)

                if exc.status == 403:
                    raise ForbiddenError(exc)

                if exc.status == 429:
                    if api_error_retries:
                        api_error_retries -= 1
                        # https://developers.criteo.com/marketing-solutions/docs/requesting-a-report
                        yield SleepIteration(sleep=60)
                        continue

                if exc.status in (500, 503):
                    # https://developers.criteo.com/marketing-solutions/docs/how-to-handle-api-errors
                    if api_error_retries:
                        api_error_retries -= 1
                        yield SleepIteration(sleep=iter_num * 20)
                        continue

                raise
            else:
                if http_code == 200:
                    if response_content:
                        response_content: HTTPResponse
                        content: str = response_content.read().decode("utf-8-sig")
                        data = orjson.loads(content)

                        if self.export.chunk_size:
                            for chunk in chunker(
                                data["Rows"],
                                size=self.export.chunk_size,
                            ):
                                yield ExportContext(
                                    columns=columns,
                                    data=chunk,
                                    data_orient=DataOrient.dict,
                                    response_kwargs={"headers": response_headers},
                                )
                        else:
                            yield ExportContext(
                                columns=columns,
                                data=data["Rows"],
                                data_orient=DataOrient.dict,
                                response_kwargs={"headers": response_headers},
                            )
                    else:
                        self.logger.warning("Didn't receive data")

                    break
                else:
                    if api_error_retries:
                        api_error_retries -= 1
                        yield SleepIteration(sleep=10)

                    raise Exception(
                        f"CriteoError: code={http_code}, headers={response_headers}\n"
                        f"response_content={response_content}"
                    )
