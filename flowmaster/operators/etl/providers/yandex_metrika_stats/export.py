import datetime as dt
from logging import Logger
from typing import TYPE_CHECKING, Optional, Iterator

from tapi_yandex_metrika import YandexMetrikaStats
from tapi_yandex_metrika.exceptions import YandexMetrikaTokenError

from flowmaster.exceptions import AuthError
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.types import DataOrient

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig


class YandexMetrikaStatsExport(ExportAbstract):

    def __init__(self, config: "ETLFlowConfig", logger: Optional[Logger] = None):
        self.credentials = config.export.credentials.dict()
        self.params = config.export.params.dict()
        super(YandexMetrikaStatsExport, self).__init__(config, logger)

    @classmethod
    def validate_params(cls, **params: dict) -> None:
        assert "ids" in params
        assert "metrics" in params

    @property
    def client(self) -> YandexMetrikaStats:
        return YandexMetrikaStats(**self.credentials)

    def collect_params(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> dict:
        params = super(YandexMetrikaStatsExport, self).collect_params(
            start_period, end_period, **self.params
        )
        if "date1" in params:
            params["date1"] = start_period.date().isoformat()

        if "date2" in params:
            params["date2"] = end_period.date().isoformat()

        return params

    def __call__(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> Iterator[ExportContext]:
        try:
            params = self.collect_params(start_period, end_period)

            self.logger.info("Exportation data")
            report = self.client.stats().get(params=params)
            for result in report().pages():
                self.logger.info("Iter export data")

                yield ExportContext(
                    export_kwargs=result().request_kwargs,
                    columns=report.columns,
                    data=result().to_columns(),
                    data_orient=DataOrient.columns,
                )

        except YandexMetrikaTokenError as ex:
            raise AuthError(ex)
