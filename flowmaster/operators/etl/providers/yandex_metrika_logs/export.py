import datetime as dt
import time
from logging import Logger
from typing import TYPE_CHECKING, Optional, Iterator

from tapi_yandex_metrika import YandexMetrikaLogsapi
from tapi_yandex_metrika.exceptions import YandexMetrikaTokenError

from flowmaster.exceptions import AuthError
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.types import DataOrient
from flowmaster.utils.thread_executor import SleepTask

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig


class YandexMetrikaLogsExport(ExportAbstract):
    def __init__(self, config: "ETLFlowConfig", logger: Optional[Logger] = None):
        self.counter_id = config.export.credentials.counter_id
        self.credentials = config.export.credentials.dict()
        self.params = config.export.params.dict()
        self.initial_interval_check_report = config.export.initial_interval_check_report
        super(YandexMetrikaLogsExport, self).__init__(config, logger)

    @classmethod
    def validate_params(cls, **params: dict) -> None:
        assert "date1" in params
        assert "date2" in params
        assert "fields" in params
        assert "source" in params

    @property
    def client(self) -> YandexMetrikaLogsapi:
        return YandexMetrikaLogsapi(
            wait_report=True,
            **self.credentials,
            default_url_params={"counterId": self.counter_id},
        )

    def collect_params(
        self, start_period: dt.datetime, end_period: dt.datetime, **params
    ) -> dict:
        params = self.params.copy()
        params["date1"] = start_period.date().isoformat()
        params["date2"] = end_period.date().isoformat()
        params["fields"] = params.pop("columns")

        return super(YandexMetrikaLogsExport, self).collect_params(
            start_period, end_period, **params
        )

    def search_identic_report(
        self, start_period: dt.datetime, end_period: dt.datetime, params: dict
    ) -> Optional[int]:
        report_info_list = self.client.allinfo().get()

        for info in report_info_list["requests"]:
            report_params = {k: v for k, v in info.items() if k in params}
            report_params = super(YandexMetrikaLogsExport, self).collect_params(
                start_period, end_period, **report_params
            )
            if params == report_params and info["status"] in ("processed", "created"):
                return info["request_id"]

    def sleeptime(self, repeat_number: int) -> int:
        max_sleep = self.initial_interval_check_report * 10
        sleep_time = repeat_number * self.initial_interval_check_report
        return sleep_time if sleep_time <= max_sleep else max_sleep

    def __call__(
        self,
        start_period: dt.datetime,
        end_period: dt.datetime,
        dry_run=False,
        **kwargs,
    ) -> Iterator[ExportContext]:
        try:
            params = self.collect_params(start_period, end_period)

            self.logger.info("Exportation data")

            request_id = self.search_identic_report(start_period, end_period, params)
            if request_id is None:
                while True:
                    # Evaluate report.
                    result = self.client.evaluate().get(params=params)

                    if result["log_request_evaluation"]["possible"] is False:
                        sleeptime = 60 * 5
                        self.logger.info(
                            f"The report store is full. "
                            f"Waiting for when to be free {sleeptime} sec."
                        )
                        if dry_run is True:
                            raise Exception("The report store is full")

                        yield SleepTask(sleep=sleeptime)
                    else:
                        # Create report.
                        result = self.client.create().post(params=params)
                        request_id = result["log_request"]["request_id"]
                        break

            # Wait report.
            repeat_number = 1
            while True:
                # Wait report.
                result = self.client.info(requestId=request_id).get()
                status = result["log_request"]["status"]

                if status == "processed":
                    self.logger.info(f"Download report")
                    report = self.client.download(requestId=request_id).get()
                    break
                elif "cleaned" in status:
                    raise Exception(
                        "The report does not exist, it has been cleared. "
                        "Curent report status is '{}'".format(status)
                    )
                else:
                    sleeptime = self.sleeptime(repeat_number)
                    self.logger.info(f"Wait report {sleeptime} sec.")
                    if dry_run is True:
                        time.sleep(10)
                    yield SleepTask(sleep=sleeptime)

                repeat_number += 1

            # Download report.
            for part in report().parts():
                self.logger.info("Iter export data")

                yield ExportContext(
                    export_kwargs=part().request_kwargs,
                    columns=report.columns,
                    data=part().to_columns(),
                    data_orient=DataOrient.columns,
                )

            if dry_run is not True:
                self.client.clean(requestId=request_id).post()

        except YandexMetrikaTokenError as ex:
            raise AuthError(ex)
