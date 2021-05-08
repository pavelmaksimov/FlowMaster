# Конфигурация экспорта из Яндекс Метрика Mangement API


### Базовые политики

```yaml
export:
    credentials:
        access_token: str
    
    resource: Literal["counters", "clients", "goals"]
    params: ...
    columns: ...
    
    # Максимальное кол-во одновременного экспорта.
    concurrency: int = 5
    # Ограничивает выполнение в случае переполнения доступных слотов хотя бы в одном из указанных пулов.
    pools: Optional[list[str]] = None
```

### Экспорт данных клиентов Яндекс.Директа
https://yandex.ru/dev/metrika/doc/api2/management/direct_clients/getclients.html
```yaml
export:
    resource: clients
    columns: list[Literal["id", "name", "chief_login"]] = [
        "id",
        "name",
        "chief_login",
    ]
```
Пример [конфигиурации](../../../examples/etl/ymm_clients-file.etl.flow.yml)

### Экспорт данных по целям
https://yandex.ru/dev/metrika/doc/api2/management/goals/goals.html
```yaml
export:
    resource: goals
    params: 
        useDeleted: Optional[bool] = None
        
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
```
Пример [конфигиурации](../../../examples/etl/ymm_goals-file.etl.flow.yml)

### Экспорт данных по счетчикам
https://yandex.ru/dev/metrika/doc/api2/management/counters/counters.html
```yaml
export:
    resource: counters
    params: 
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
```
Пример [конфигиурации](../../../examples/etl/ymm_counters-file.etl.flow.yml)
