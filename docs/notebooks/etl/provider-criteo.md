# Конфигурация экспорта из Criteo

- Пример [конфигиурации](../../../examples/etl/criteo-clickhouse.etl.yml) загрузки в Clickhouse

```yaml
...
provider: "criteo"
export:
    # Required policies:

    credentials:
        # https://developers.criteo.com/marketing-solutions/docs/onboarding-checklist
        # From API key file.
        client_id: str
        client_secret: str
    api_version: Literal["202104"]
    resource: Literal["stats"]
    params: 
        # https://developers.criteo.com/marketing-solutions/docs/dimensions
        dimensions: list[str]
        # https://developers.criteo.com/marketing-solutions/docs/metrics
        metrics: list[str]
        # https://developers.criteo.com/marketing-solutions/docs/currencies-supported
        currency: str
        
        # Optional policy:
        
        # If you do not provide any advertiserIds value,
        # statistics for all advertisers in your portfolio will be returned.
        advertiser_ids: Optional[list[str]] = None
        # https://developers.criteo.com/marketing-solutions/docs/timezones-supported
        timezone: str = "UTC"
    
    # Optional policy:

    # Number of lines to read per iteration.
    chunk_size: Optional[PositiveInt]
    # Maximum number of simultaneous exports.
    concurrency: PositiveInt = 5
    # Limits execution in case of overflow of available slots in at least one of the specified pools.
    pools: Optional[list[str]] = None
...
```
