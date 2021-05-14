from typing import Literal

from pydantic import BaseModel

from flowmaster.operators.base.policy import BasePolicy


class YandexMetrikaLogsExportPolicy(BasePolicy):
    class CredentialsPolicy(BaseModel):
        counter_id: int
        access_token: str

    class ParamsPolicy(BaseModel):
        source: Literal["visits", "hits"]
        columns: list[str]

    credentials: CredentialsPolicy
    params: ParamsPolicy
    initial_interval_check_report: int = 60
    concurrency: int = 3
