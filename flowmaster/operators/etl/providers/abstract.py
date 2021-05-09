import datetime as dt
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator, Optional, Type

import jinja2

if TYPE_CHECKING:
    from flowmaster.operators.base.config import PydanticModelT
    from flowmaster.operators.etl.config import ETLFlowConfig
    from flowmaster.operators.etl.transform import Transform
    from flowmaster.operators.etl.dataschema import ExportContext


class ExportAbstract(ABC):
    def __init__(
        self, config: "ETLFlowConfig", logger: Optional[logging.Logger] = None
    ):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

    @classmethod
    def validate_params(cls, **params: dict) -> None:
        ...

    def collect_params(
        self, start_period: dt.datetime, end_period: dt.datetime, **params
    ) -> dict:
        new_params = {}
        for key, value in params.items():
            if value is not None:
                if isinstance(value, list):
                    value = ",".join(map(str, value))

                new_params[key] = value

                if isinstance(value, str):
                    new_params[key] = jinja2.Template(value).render(
                        **{
                            "start_period": start_period,
                            "end_period": end_period,
                            "datetime": dt.datetime.now(),
                        }
                    )

        self.validate_params(**new_params)

        return new_params

    @abstractmethod
    def __call__(self, *args, **kwargs) -> Iterator["ExportContext"]:
        ...


class ProviderAbstract(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    @abstractmethod
    def policy_model(self) -> "PydanticModelT":
        ...

    @property
    @abstractmethod
    def export_class(self) -> Type[ExportAbstract]:
        ...

    @property
    def transform_class(self) -> Type["Transform"]:
        from flowmaster.operators.etl.transform.service import Transform

        return Transform

    def __init__(self, *args, **kwargs):
        self.Export = self.export_class(*args, **kwargs)
        self.Transform = self.transform_class(*args, **kwargs)
