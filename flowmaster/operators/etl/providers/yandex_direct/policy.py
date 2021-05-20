from typing import Union, Optional, Literal

from pydantic import BaseModel, Field

from flowmaster.operators.base.policy import BasePolicy


class YandexDirectExportPolicy(BasePolicy):
    class CredentialsPolicy(BaseModel):
        access_token: str
        # Login of the advertiser - the client of the advertising agency.
        # Mandatory if the request is made on behalf of the agency.
        login: Optional[str] = None

    class HeadersPolicy(BaseModel):
        # https://yandex.ru/dev/direct/doc/reports/headers.html
        # Spend agency points, not advertiser points, when fulfilling a request.
        use_operator_units: Optional[bool] = None
        return_money_in_micros: Optional[bool] = None

    class BodyPolicy(BaseModel):
        """
        Example body for get campaigns: https://yandex.ru/dev/direct/doc/ref-v5/campaigns/get.html#input
        """

        method: str
        params: dict

    class ReportBodyPolicy(BaseModel):
        """
        https://yandex.ru/dev/direct/doc/reports/spec.html
        """

        class ReportParamsPolicy(BaseModel):
            class SelectionCriteriaPolicy(BaseModel):
                class FilterPolicy(BaseModel):
                    Field: str
                    Operator: str
                    Values: list[str]

                Filter: list[FilterPolicy] = Field(default_factory=list)

            class PagePolicy(BaseModel):
                Limit: int

            class OrderByPolicy(BaseModel):
                Field: str
                SortOrder: Optional[Literal["ASCENDING", "DESCENDING"]] = None

            SelectionCriteria: SelectionCriteriaPolicy = Field(default_factory=dict)
            ReportType: str
            DateRangeType: str
            FieldNames: list[str]
            IncludeVAT: Literal["YES", "NO"]
            IncludeDiscount: Optional[Literal["YES", "NO"]] = None
            Goals: Optional[list[str]] = None
            AttributionModel: Optional[str] = None
            Page: Optional[PagePolicy] = None
            OrderBy: Optional[OrderByPolicy] = None

        params: ReportParamsPolicy

    credentials: CredentialsPolicy
    resource: Union[
        Literal["reports"],
        Literal[
            "adextensions",
            "adgroups",
            "adimages",
            "ads",
            "agencyclients",
            "audiencetargets",
            "bidmodifiers",
            "bids",
            "businesses",
            "campaigns",
            "changes",
            "clients",
            "creatives",
            "dictionaries",
            "dynamicads",
            "feeds",
            "keywordbids",
            "keywords",
            "keywordsresearch",
            "leads",
            "negativekeywordsharedsets",
            "retargeting",
            "sitelinks",
            "smartadtargets",
            "turbopages",
            "vcards",
        ],
    ]
    body: Union[BodyPolicy, ReportBodyPolicy]
    headers: Optional[HeadersPolicy] = None
    concurrency: int = 5
