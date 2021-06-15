# Конфигурация экспорта из CSV файла

```yaml
...
provider: "sqlite"
export:
    # Required policies:
    
    db_path: str# 
    table: str
    columns: list[str]
    
    # Optional policies:

    where: str = ""
    order_by: str = ""
    
    # Custom query.
    sql: Optional[str] = None
    
    # Number of lines to read per iteration.
    chunk_size: PositiveInt = 10000
    
    sql_before: Optional[list[str]] = None
    sql_after: Optional[list[str]] = None

    # Maximum number of simultaneous exports.
    concurrency: int = 1
    # Limits execution in case of overflow of available slots in at least one of the specified pools.
    pools: Optional[list[str]] = None
...
```
