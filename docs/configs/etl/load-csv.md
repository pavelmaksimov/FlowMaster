# Конфигурация экспорта из CSV файла

```yaml
...
provider: "csv"
export:
    # Required policies:
    
    # Path to file.
    file_path: str# 
    # Is there a row of column names in the file.
    with_columns: bool
    # Column names.
    columns: list[str]
    
    # Optional policies:
    
    # Column separator.
    sep: str = "\t"
    # Line separator.
    newline: str = "\n"
    # File encoding.
    encoding: str = "UTF-8"
    # The number of lines above to skip.
    skip_begin_lines: int = 0
    # Number of lines to read per iteration.
    chunk_size: PositiveInt = 10000

    # Maximum number of simultaneous exports.
    concurrency: int = 1
    # Limits execution in case of overflow of available slots in at least one of the specified pools.
    pools: Optional[list[str]] = None
...
```

### export.file_path
Путь к файлу.


### export.with_columns
Есть ли строка имен колонок в файле.


### export.columns
Имена колонок. \
Если в файле есть строка с именами колонок, они должны быть одинаковыми.


### export.with_columns
Есть ли названия колонок в данных.


### export.skip_begin_lines
Кол-во строк сверху, которые нужно пропустить.


### export.chunk_size
Кол-во строк для чтения в итерации.
