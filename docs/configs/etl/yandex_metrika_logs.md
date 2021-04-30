# Конфигурация экспорта из Яндекс Метрика Logs API

- Пример [конфигиурации](../../../examples/etl/yml-clickhouse.etl.flow.yml) загрузки в Clickhouse
- Пример [конфигиурации](../../../examples/etl/yml-file.etl.flow.yml) сохранение в файл

```yaml
export:
    credentials:
        counter_id: int
        access_token: str

    params:
        source: Literal["visits", "hits"]
        # Доступные колонки:
        # visits - https://yandex.ru/dev/metrika/doc/api2/logs/fields/visits.html
        # hits - https://yandex.ru/dev/metrika/doc/api2/logs/fields/hits.html
        columns: list[str]
    
    # Начальный интервал проверки готовности отчета, далее каждый раз увеличивается, 
    # но не более, чем на (initial_interval_check_report * 10)
    initial_interval_check_report: int = 60
    # Максимальное кол-во одновременного экспорта.
    concurrency: int = 3
    # Ограничивает выполнение в случае переполнения доступных слотов хотя бы в одном из указанных пулов.
    pools: Optional[list[str]] = None
```

#### Начальный интервал проверки готовности отчета initial_interval_check_report

Далее с каждой следующей проверке интервал увеличивается в 2 раза, 
но не более (initial_interval_check_report * 10)
