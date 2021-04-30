from typing import Optional, Union

from pydantic import PositiveInt, NegativeInt

from flowmaster.operators.base.policy import BaseWorkPolicy


class ETLWorkPolicy(BaseWorkPolicy):
    update_stale_data: Optional[Union[PositiveInt, list[NegativeInt]]] = None
