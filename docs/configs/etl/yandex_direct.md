# Конфигурация экспорта из Яндекс Директ

```yaml
export:
    credentials:
        access_token: str
        # Login of the advertiser - the client of the advertising agency.
        # Mandatory if the request is made on behalf of the agency.
        login: Optional[str] = None

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
    # Example body for get campaigns: https://yandex.ru/dev/direct/doc/ref-v5/campaigns/get.html#input
    # Example body for get report: https://yandex.ru/dev/direct/doc/reports/spec.html
    body: dict
    headers:
        # https://yandex.ru/dev/direct/doc/reports/headers.html
        # Spend agency points, not advertiser points, when fulfilling a request.
        use_operator_units: Optional[bool] = None
        # If not specified or False, monetary values are returned as whole numbers 
        # currency amounts multiplied by 1,000,000.
        return_money_in_micros: Optional[bool] = None

    # Максимальное кол-во одновременного экспорта.
    concurrency: int = 5
    # Ограничивает выполнение в случае переполнения доступных слотов хотя бы в одном из указанных пулов.
    pools: Optional[list[str]] = None
```
