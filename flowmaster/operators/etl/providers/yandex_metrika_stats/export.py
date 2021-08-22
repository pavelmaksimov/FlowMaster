import datetime as dt
from typing import TYPE_CHECKING, Optional, Iterator

from flowmaster.exceptions import AuthError
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.utils.logging_helper import Logger

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook


class YandexMetrikaStatsExport(ExportAbstract):
    def __init__(self, notebook: "ETLNotebook", logger: Optional[Logger] = None):
        from tapi_yandex_metrika import YandexMetrikaStats

        self.credentials = notebook.export.credentials.dict()
        self.params = notebook.export.params.dict()
        self.client = YandexMetrikaStats(**self.credentials)
        super(YandexMetrikaStatsExport, self).__init__(notebook, logger)

    @classmethod
    def validate_params(cls, **params: dict) -> None:
        assert "ids" in params
        assert "metrics" in params

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
        from tapi_yandex_metrika.exceptions import YandexMetrikaTokenError

        try:
            params = self.collect_params(start_period, end_period)

            self.logger.info("Exportation data")
            report = self.client.stats().get(params=params)
            for result in report().pages():
                self.logger.info("Iter export data")

                yield ExportContext(
                    request_kwargs=result().request_kwargs,
                    columns=report.columns,
                    data=result().to_columns(),
                    data_orient=DataOrient.columns,
                )

        except YandexMetrikaTokenError as ex:
            raise AuthError(ex)
