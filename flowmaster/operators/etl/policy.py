from typing import Optional, Union

from pydantic import PositiveInt, NegativeInt, validator, BaseModel

from flowmaster.operators.base.policy import FlowConfig, PydanticModelT
from flowmaster.operators.etl.loaders import storage_classes, ClickhouseLoader
from flowmaster.operators.etl.providers import provider_classes


class ETLFlowConfig(FlowConfig):
    class ETLWorkPolicy(FlowConfig.WorkPolicy):
        update_stale_data: Optional[Union[PositiveInt, list[NegativeInt]]] = None

    provider: str
    storage: str
    work: ETLWorkPolicy
    export: PydanticModelT
    load: PydanticModelT
    transform: PydanticModelT

    @validator("provider")
    def _validate_provider(cls, provider, values, **kwargs):
        assert provider in provider_classes.keys()
        return provider

    @validator("storage")
    def _validate_storage(cls, storage, values, **kwargs):
        assert storage in storage_classes.keys()
        return storage

    def _set_partition_columns(self, **kwargs) -> None:
        transform = kwargs.get("transform", {})
        load = kwargs.get("load", {})

        # For Clickhouse.
        if kwargs.get("storage") == ClickhouseLoader.name and isinstance(load, dict):
            transform["partition_columns"] = load.get("table_schema", {}).get(
                "partition"
            )

    def _set_column_map(self, **kwargs) -> None:
        transform = kwargs.get("transform", {})
        load = kwargs.get("load", {})

        # For Clickhouse.
        if kwargs.get("storage") == ClickhouseLoader.name and isinstance(load, dict):
            if "column_map" not in transform:
                columns_schema = load.get("table_schema", {}).get("columns", None)
                if columns_schema:
                    try:
                        transform["column_map"] = {
                            k: v.split(" ")[0] for k, v in columns_schema.items()
                        }
                    except IndexError:
                        raise ValueError("Invalid table column schema")

    def __init__(self, **kwargs):
        self._set_partition_columns(**kwargs)
        self._set_column_map(**kwargs)

        super().__init__(**kwargs)

        # Checking if the input data are not dictionaries.
        if isinstance(transform := kwargs.get("transform", {}), BaseModel):
            transform = transform.dict()
        self.transform = storage_classes[self.storage].transform_policy_model(
            **transform
        )

        if isinstance(load := kwargs.get("load", {}), BaseModel):
            load = load.dict()
        self.load = storage_classes[self.storage].policy_model(**load)

        if isinstance(export := kwargs.get("export", {}), BaseModel):
            export = export.dict()
        self.export = provider_classes[self.provider].policy_model(**export)
