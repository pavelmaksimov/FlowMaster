# Конфигурация экспорта из Google Sheets

- Пример [конфигиурации](../../../examples/etl/google_sheets-csv.etl.yml) экспорт данных в файл CSV

```yaml
...
provider: "google_sheets"
export:
    # Required policies:
    
    # From URL https://docs.google.com/spreadsheets/d/1sb0avK6C34kf2pVARc5bAVLCK9pbF80c_hWTm10kRNU/edit#gid=0
    sheet_id: str
    columns: list[str]
    # If there is a row with column names.
    with_columns: bool
    # From URL https://docs.google.com/spreadsheets/d/1sb0avK6C34kf2pVARc5bAVLCK9pbF80c_hWTm10kRNU/edit#gid=0
    page_id: int = 0
    
    # Optional policies:

    start: Optional[tuple[IndexRowInt, IndexColumnInt]] = None
    end: Optional[tuple[IndexRowInt, IndexColumnInt]] = None

    # Maximum number of simultaneous exports.
    concurrency: int = 1
    # Limits execution in case of overflow of available slots in at least one of the specified pools.
    pools: Optional[list[str]] = None
...
```
