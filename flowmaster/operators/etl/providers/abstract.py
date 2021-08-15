import datetime as dt
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator, Optional, Type, Union

import jinja2
import pendulum
from pydantic import BaseModel

from flowmaster.utils.logging_helper import Logger, getLogger

if TYPE_CHECKING:
    from flowmaster.operators.base.policy import PydanticModelT
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.transform import Transform
    from flowmaster.operators.etl.dataschema import ExportContext
    from flowmaster.executors import SleepIteration


class ExportAbstract(ABC):
    def __init__(self, notebook: "ETLNotebookPolicy", logger: Optional[Logger] = None):
        self.notebook = notebook
        self.logger = logger or getLogger()

    @classmethod
    def validate_params(cls, **params: dict) -> None:
        # TODO: drop method
        ...

    def collect_params(
        self, start_period: dt.datetime, end_period: dt.datetime, **params
    ) -> dict:
        # TODO: drop method
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
                            "datetime": pendulum.now("local"),
                        }
                    )

        self.validate_params(**new_params)

        return new_params

    def model_templating(
        self, start_period: dt.datetime, end_period: dt.datetime, model: BaseModel
    ):
        # TODO: replace method with function
        new_model_dict = {}
        for key, value in model.dict(exclude_unset=True).items():
            new_model_dict[key] = value

            if isinstance(value, str):
                new_model_dict[key] = jinja2.Template(value).render(
                    **{
                        "start_period": start_period,
                        "end_period": end_period,
                        "datetime": pendulum.now("UTC"),
                        "name": self.notebook.name,
                        "provider": self.notebook.provider,
                        "storage": self.notebook.storage,
                    }
                )

        self.validate_params(**new_model_dict)

        return model.parse_obj(new_model_dict)

    @abstractmethod
    def __call__(
        self, *args, **kwargs
    ) -> Iterator[Union["ExportContext", "SleepIteration"]]:
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
