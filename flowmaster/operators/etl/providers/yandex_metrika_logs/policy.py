from typing import Literal

from pydantic import BaseModel

from flowmaster.operators.base.policy import BasePolicy


class YandexMetrikaLogsExportPolicy(BasePolicy):
    class Credentials(BaseModel):
        counter_id: int
        access_token: str

    class Params(BaseModel):
        source: Literal["visits", "hits"]
        columns: list[str]

    credentials: Credentials
    params: Params
    initial_interval_check_report: int = 60
    concurrency: int = 3
