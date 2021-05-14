from typing import Union, List, Optional, Literal

from pydantic import BaseModel

from flowmaster.operators.base.policy import BasePolicy


class YandexMetrikaStatsExportPolicy(BasePolicy):
    class CredentialsPolicy(BaseModel):
        access_token: str

    class ParamsPolicy(BaseModel):
        # https://yandex.ru/dev/metrika/doc/api2/api_v1/data.html
        ids: Union[str, int, List[str], List[int]]
        metrics: Union[str, List[str]]
        dimensions: Optional[Union[str, List[str]]] = None
        date1: Literal[True] = None
        date2: Literal[True] = None
        limit: Optional[Union[int, str]] = None
        accuracy: Optional[Union[float, str]] = None
        direct_client_logins: Optional[Union[str, int, List[str], List[int]]] = None
        filters: Optional[str] = None
        include_undefined: Optional[bool] = None
        lang: Optional[str] = None
        preset: Optional[str] = None
        pretty: Optional[bool] = None
        proposed_accuracy: Optional[bool] = False
        sort: Optional[Union[str, List[str]]] = None
        timezone: Optional[str] = None

    credentials: CredentialsPolicy
    params: ParamsPolicy
    concurrency: int = 3
