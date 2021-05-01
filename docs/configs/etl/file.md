# Сохранение данных в файл

Примеры [конфигураций](../../../examples/etl/)

```yaml
load:
    # Обязательные политики:
    # "w" При каждой загрузке файл перезаписывается.
    # "a" Данные вставляются в конец файла.
    save_mode: Literal["a", "w"]
    # Через шаблонизатор jinja доступны {{provider}} {{storage}} {{name}} {{datetime}}.
    file_name: str = "{{provider}} {{storage}}  {{name}}.tsv"
    # Optional:
    # По умолчанию файлы сохраняются в FlowMaster/file_storage
    path: str = FILE_STORAGE_DIR
    encoding: str = "UTF-8"
    sep: str = "\t"
    newline: str = "\n"
    # Добавить названия столбцов в первую строку файла.
    with_columns: bool = True
    # Добавление текста перед вставляемыми данными.
    # Через шаблонизатор jinja доступны {{provider}} {{storage}} {{name}} {{datetime}}.
    add_data_before: str = ""
    # Добавление текста после вставляемых данных.
    # Через шаблонизатор jinja доступны {{provider}} {{storage}} {{name}} {{datetime}}.
    add_data_after: str = ""
    # Максимальное кол-во одновременных запущенных вставок данных в файл.
    concurrency: int = 1
    # Ограничивает выполнение в случае переполнения доступных слотов хотя бы в одном из указанных пулов.
    pools: Optional[list[str]] = None

transform:
    # Обязательные политики:
    error_policy: Literal["raise", "default", "coerce", "ignore"]
    # Опциональные политики:
    timezone: Optional[str] = None
    concurrency: int = 100
    pools: Optional[list[str]] = None
    column_schema:
        ExportColName: # Название колонки, которое экспортируется.
            # Опциональные политики:
            # Новое название колонки.
            name: Optional[str] = None
            # Тип данных.
            dtype: Optional[
                Literal[
                    "string",
                    "array",
                    "int",
                    "uint",
                    "float",
                    "date",
                    "datetime",
                    "timestamp",
                ]
            ] = None
            errors: Optional[Literal["raise", "default", "coerce", "ignore"]] = None
            # формат даты для десериализации из строки.
            dt_format: Optional[str] = None
            allow_null: Optional[bool] = None
            null_values: Optional[list] = None
            clear_values: Optional[list] = None
```


### Политика поведения при возникновении ошибки в преобразовании данных (transform.error_policy)

- **raise** - при ошибке преобразования вызовет Exception
- **default** - при ошибке преобразования поставит значение по умолчанию, 
  в зависимости от типа столбца
- **ignore** - при ошибке преобразования, проигнорирует. 
  Но вероятно при вставке данных, вызовется Exception
- **coerce** - при ошибке поставит null значение


### Политики преобразования колонки (transform.column_schema)

Здесь указываются параметры преобразования колонки.
Все политики опциональны, поэтому можно добавлять только необходимые.

- **name** - новое название колонки 

- **dtype** - тип данных

- **errors** - значения, как и в error_policy
  
- **dt_format** - формат даты для десериализации из строки

- **null_values** - указанные значения будет воспринимать, 
  как null и преобразует в него. Например если указать значение "--", 
  то если встретит его, преобразует в null.

- **clear_values** - очищает указанные значение. 
  Если столбец Nullable, то преобразует в null, если нет, 
  то в значение по умолчанию в зависимости от типа столбца.

- **allow_null** - Если True, то будет ставить пустое значение, 
  если не сможет преобразовать и если встретит значение из **clear_values** и **null_values**.
  Если False будет ставить значение по умолчанию в зависимости от типа колонки.
