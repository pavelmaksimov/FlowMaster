from typing import Literal, Optional, TypeVar

from pydantic import BaseModel, Field

from flowmaster.operators.base.policy import BasePolicy

ErrorPolicyLiteralT = TypeVar(
    "ErrorPolicyLiteralT", bound=Literal["raise", "default", "coerce", "ignore"]
)
DTypeLiteralT = TypeVar(
    "DTypeLiteralT",
    bound=Literal[
        "array", "timestamp", "string", "uint", "int", "float", "datetime", "date"
    ],
)


class BaseTransformPolicy(BasePolicy):
    error_policy: ErrorPolicyLiteralT
    timezone: Optional[str] = None
    partition_columns: Optional[list] = None
    column_schema: dict[str, BaseModel] = Field(default_factory=dict)
    concurrency: int = 100
