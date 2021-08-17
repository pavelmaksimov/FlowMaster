from flowmaster.operators.etl.transform.tschema import (
    FileTransformSchema,
    ClickhouseTransformSchema,
)
from flowmaster.utils import KlassCollection

TransformSchemas = KlassCollection(FileTransformSchema, ClickhouseTransformSchema)
