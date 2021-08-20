import hashlib
from typing import Optional, Union

from pydantic import PositiveInt, NegativeInt, validator, BaseModel

from flowmaster.operators.base.policy import BaseNotebook, PydanticModelT
from flowmaster.operators.etl.loaders import Loaders
from flowmaster.operators.etl.providers import Providers


class ETLNotebook(BaseNotebook):
    class WorkPolicy(BaseNotebook.WorkPolicy):
        update_stale_data: Optional[Union[PositiveInt, list[NegativeInt]]] = None

    provider: str
    storage: str
    work: Optional[WorkPolicy] = None
    export: PydanticModelT
    load: PydanticModelT
    transform: PydanticModelT

    @validator("provider")
    def _validate_provider(cls, provider, values, **kwargs):
        assert provider in Providers
        return provider

    @validator("storage")
    def _validate_storage(cls, storage, values, **kwargs):
        assert storage in Loaders
        return storage

    def _set_partition_columns(self, **kwargs) -> None:
        transform = kwargs.get("transform", {})
        load = kwargs.get("load", {})

        # For Clickhouse.
        if (
            isinstance(load, dict)
            and kwargs.get("storage") == Loaders.ClickhouseLoader.name
        ):
            transform["partition_columns"] = load.get("table_schema", {}).get(
                "partition"
            )

    def _set_column_map(self, **kwargs) -> None:
        transform = kwargs.get("transform", {})
        load = kwargs.get("load", {})

        # For Clickhouse.
        if (
            isinstance(load, dict)
            and kwargs.get("storage") == Loaders.ClickhouseLoader.name
        ):
            if "column_map" not in transform:
                columns_schema = load.get("table_schema", {}).get("columns", None)
                if columns_schema:
                    try:
                        transform["column_map"] = {
                            k: v.split(" ")[0] for k, v in columns_schema.items()
                        }
                    except IndexError:
                        raise ValueError("Invalid table column schema")

    def _set_hash(self):
        self.hash = hashlib.md5(
            str(
                (
                    self.name,
                    self.load.dict(exclude_unset=True),
                    self.transform.dict(exclude_unset=True),
                    self.export.dict(exclude_unset=True),
                )
            ).encode()
        ).hexdigest()

    def __init__(self, **kwargs):
        self._set_partition_columns(**kwargs)
        self._set_column_map(**kwargs)

        super().__init__(**kwargs)

        self._set_hash()

        # Checking if the input data are not dictionaries.
        if isinstance(transform := kwargs.get("transform", {}), BaseModel):
            transform = transform.dict()
        self.transform = Loaders[self.storage].transform_policy_model(**transform)

        if isinstance(load := kwargs.get("load", {}), BaseModel):
            load = load.dict()
        self.load = Loaders[self.storage].policy_model(**load)

        if isinstance(export := kwargs.get("export", {}), BaseModel):
            export = export.dict()
        self.export = Providers[self.provider].policy_model(**export)

    class Config:
        operator = "etl"
