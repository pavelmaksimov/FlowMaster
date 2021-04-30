from typing import Optional
from typing import TypeVar

from pydantic import BaseModel

from flowmaster.operators.base.policy import BaseWorkPolicy

PydanticModelT = TypeVar("PydanticModelT", bound=BaseModel)


class BaseFlowConfig(BaseModel):
    class Work(BaseWorkPolicy):
        ...

    name: str
    work: BaseWorkPolicy
    description: Optional[str] = None
