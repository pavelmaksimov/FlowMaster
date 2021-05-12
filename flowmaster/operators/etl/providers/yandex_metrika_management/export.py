import datetime as dt
from logging import Logger
from typing import TYPE_CHECKING, Iterator, Optional, Literal

from tapi_yandex_metrika import YandexMetrikaManagement
from tapi_yandex_metrika.exceptions import YandexMetrikaTokenError
from tapi_yandex_metrika.resource_mapping import MANAGEMENT_RESOURCE_MAPPING

from flowmaster.exceptions import AuthError
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.types import DataOrient

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig


class YandexMetrikaManagementExport(ExportAbstract):
    class ResourceNames:
        counters = "counters"
        clients = "clients"
        goals = "goals"
        LiteralT = Literal["counters", "clients", "goals"]

    def __init__(self, config: "ETLFlowConfig", logger: Optional[Logger] = None):
        self.resource = config.export.resource
        self.columns = config.export.columns
        self.params = config.export.params.dict()
        self.credentials = config.export.credentials.dict()
        super(YandexMetrikaManagementExport, self).__init__(config, logger)

    @property
    def client(self) -> YandexMetrikaManagement:
        return YandexMetrikaManagement(**self.credentials)

    def get_counter_ids(self) -> list:
        result = self.client.counters().get()
        data = self.processing_response_data(
            self.ResourceNames.counters, result().data, filter_columns=["id"]
        )
        counters_ids = [i["id"] for i in data]
        return counters_ids

    def collect_params(
        self, start_period: dt.datetime, end_period: dt.datetime, **params
    ) -> Iterator[tuple[dict, dict]]:
        get_params = self.params.copy()
        url_params = {}

        if self.resource == self.ResourceNames.clients:
            get_params.update(counters=self.get_counter_ids())
            yield url_params, super(YandexMetrikaManagementExport, self).collect_params(
                start_period, end_period, **get_params
            )

        elif self.resource == self.ResourceNames.goals:
            for counter_id in self.get_counter_ids():
                url_params.update(counterId=counter_id)
                yield (
                    url_params,
                    super(YandexMetrikaManagementExport, self).collect_params(
                        start_period, end_period, **get_params
                    ),
                )
        else:
            yield url_params, super(YandexMetrikaManagementExport, self).collect_params(
                start_period, end_period, **get_params
            )

    def processing_response_data(
        self, resource: str, response_data: dict, filter_columns: list
    ) -> list[dict]:
        key = MANAGEMENT_RESOURCE_MAPPING[resource]["response_data_key"]
        data = response_data[key]
        for row in data:
            for field in list(row.keys()):
                if field not in filter_columns:
                    row.pop(field)

        return data

    def __call__(
        self, start_period: dt.datetime, end_period: dt.datetime, **kwargs
    ) -> Iterator[ExportContext]:
        for url_params, get_params in self.collect_params(start_period, end_period):
            method = getattr(self.client, self.resource)
            try:
                self.logger.info("Exportation data")
                result = method(**url_params).get(params=get_params)

                data = self.processing_response_data(
                    self.resource, result().data, self.columns
                )
                yield ExportContext(
                    export_kwargs=result().request_kwargs,
                    columns=self.columns,
                    data=data,
                    data_orient=DataOrient.dict,
                )

            except YandexMetrikaTokenError as ex:
                raise AuthError(ex)
