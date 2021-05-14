from typing import Union, Literal, Optional, ClassVar

from pydantic import BaseModel

from flowmaster.operators.base.policy import BasePolicy
from flowmaster.operators.etl.providers.yandex_metrika_management.export import (
    YandexMetrikaManagementExport as Api,
)


class BaseExportPolicy(BasePolicy):
    class CredentialsPolicy(BaseModel):
        access_token: str

    credentials: CredentialsPolicy
    resource: Api.ResourceNames.LiteralT
    concurrency: int = 5


class CountersExportPolicy(BaseExportPolicy):
    class ParamsPolicy(BaseModel):
        per_page: Optional[int] = None
        connect_status: Optional[
            Literal["NOT_CONNECTED", "READY_TO_CONNECT", "CONNECTED"]
        ] = None
        favorite: Optional[Union[bool, int]] = None
        label_id: Optional[Union[int]] = None
        permission: Optional[Literal["own", "view", "edit"]] = None
        reverse: Optional[bool] = None
        search_string: Optional[str] = None
        sort: Optional[
            Literal["None", "Default", "Visits", "Hits", "Uniques", "Name"]
        ] = None
        status: Optional[Literal["Active", "Deleted"]] = None
        type: Optional[Literal["simple", "partner"]] = None

    params: ParamsPolicy = BaseModel()
    columns: list[
        Literal[
            "code_options",
            "code_status",
            "code_status_info",
            "connect_status",
            "create_time",
            "favorite",
            "filters",
            "gdpr_agreement_accepted",
            "goals",
            "grants",
            "id",
            "labels",
            "mirrors2",
            "name",
            "operations",
            "organization_id",
            "organization_name",
            "owner_login",
            "permission",
            "site2",
            "status",
            "time_zone_name",
            "time_zone_offset",
            "type",
            "webvisor",
        ]
    ] = [
        "code_options",
        "code_status",
        "code_status_info",
        "connect_status",
        "create_time",
        "favorite",
        "filters",
        "gdpr_agreement_accepted",
        "goals",
        "grants",
        "id",
        "labels",
        "mirrors2",
        "name",
        "operations",
        "organization_id",
        "organization_name",
        "owner_login",
        "permission",
        "site2",
        "status",
        "time_zone_name",
        "time_zone_offset",
        "type",
        "webvisor",
    ]


class GoalsExportPolicy(BaseExportPolicy):
    class ParamsPolicy(BaseModel):
        useDeleted: Optional[bool] = None

    params: ParamsPolicy = BaseModel()
    columns: Optional[
        list[
            Literal[
                "id",
                "name",
                "type",
                "is_retargeting",
                "flag",
                "conditions",
                "steps",
                "depth",
                "default_price",
            ]
        ]
    ] = [
        "id",
        "name",
        "type",
        "is_retargeting",
        "flag",
        "conditions",
        "steps",
        "depth",
        "default_price",
    ]


class ClientsExportPolicy(BaseExportPolicy):
    columns: list[Literal["id", "name", "chief_login"]] = [
        "id",
        "name",
        "chief_login",
    ]
    params: ClassVar = BaseModel()


class YandexMetrikaManagementExportPolicy(BaseModel):
    def __new__(
        cls, **kwargs
    ) -> Union[GoalsExportPolicy, CountersExportPolicy, ClientsExportPolicy]:
        resource = kwargs.get("resource")
        if resource == Api.ResourceNames.goals:
            return GoalsExportPolicy(**kwargs)
        elif resource == Api.ResourceNames.counters:
            return CountersExportPolicy(**kwargs)
        elif resource == Api.ResourceNames.clients:
            return ClientsExportPolicy(**kwargs)
        else:
            raise NotImplementedError(
                f"'{resource}' resource not supported. "
                f"Permitted: {Api.ResourceNames.LiteralT.__args__}"
            )
