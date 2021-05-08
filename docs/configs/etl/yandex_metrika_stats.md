# Конфигурация экспорта из отчетов Яндекс Метрика

- Пример [конфигиурации](../../../examples/etl/yms-clickhouse.etl.flow.yml) загрузки в Clickhouse
- Пример [конфигиурации](../../../examples/etl/yms-file.etl.flow.yml) сохранение в файл

```yaml
export:
    credentials:
        access_token: str

    params:
        # Описание параметров https://yandex.ru/dev/metrika/doc/api2/api_v1/data.html
        # Required:
        ids: Union[str, int, List[str], List[int]]
        metrics: Union[str, List[str]]
        # Optional:
        date1: Literal[True] = None
        date2: Literal[True] = None
        dimensions: Optional[Union[str, List[str]]] = None
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

    # Максимальное кол-во одновременного экспорта.
    concurrency: int = 3
    # Ограничивает выполнение в случае переполнения доступных слотов хотя бы в одном из указанных пулов.
    pools: Optional[list[str]] = None
```
