from typing import Union, Type

from flowmaster.operators.base.policy import BaseNotebook
from .tschema import FileTransformSchema, ClickhouseTransformSchema


class KlassCollection:
    def __getitem__(
        self, name: str
    ) -> Type[FileTransformSchema, ClickhouseTransformSchema]: ...
    def get(
        self, name: str
    ) -> Type[FileTransformSchema, ClickhouseTransformSchema]: ...
    def init(
        self,
        storage_name: str,
        notebook: BaseNotebook,
        null_values: Union[list, tuple, set],
    ) -> Union[FileTransformSchema, ClickhouseTransformSchema]: ...
    ClickhouseTransformSchema: Type[ClickhouseTransformSchema]
    FileTransformSchema: Type[FileTransformSchema]

TransformSchemas: KlassCollection
