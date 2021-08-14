# Конфигурация экспорта из БД MySQL

```yaml
...
provider: "mysql"
export:
    # Required policies:
    
    user: str
    password: str
    host: str
    port: PositiveInt = 3306
    database: str
    table: str
    columns: list[str]
    
    # Optional policies:

    where: str = ""
    order_by: str = ""
    
    # Custom export query.
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
