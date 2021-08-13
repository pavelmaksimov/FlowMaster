# Конфигурация экспорта из Flowmaster


## Items
```yaml
...
provider: "flowmaster"
export:
    # Required policies:

    resource: "items"
    columns:
        - name
        - worktime
        - status
        - data
        - notebook_hash
        - retries
        - duration
        - info
        - started_utc
        - finished_utc
        - created_utc
        - updated_utc
    
    # Optional policies:
    
    # For 'items' resource.
    export_mode: Literal["all", "by_date"] = "all"
    # Maximum number of simultaneous exports.
    concurrency: int = 1
    # Limits execution in case of overflow of available slots in at least one of the specified pools.
    pools: Optional[list[str]] = None
...
```

## Pools
```yaml
...
provider: "flowmaster"
export:
    # Required policies:

    resource: "pools"
    columns:
        - name
        - size
        - limit
        - datetime
    
    # Optional policies:

    # Maximum number of simultaneous exports.
    concurrency: int = 1
    # Limits execution in case of overflow of available slots in at least one of the specified pools.
    pools: Optional[list[str]] = None
...
```

## Queues
```yaml
...
provider: "flowmaster"
export:
    # Required policies:

    resource: "queues"
    columns:
        - name
        - size
        - datetime
    
    # Optional policies:

    # Maximum number of simultaneous exports.
    concurrency: int = 1
    # Limits execution in case of overflow of available slots in at least one of the specified pools.
    pools: Optional[list[str]] = None
...
```
