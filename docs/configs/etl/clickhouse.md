# Вставка данных в БД Clickhouse

Примеры [конфигураций](../../../examples/etl/)

```yaml
load:
    credentials:
        # Обязательные политики:
        user: str
        host: str
        # Опциональные политики:
        port: int = 9000
        password: Optional[Union[str, int]] = None

    table_schema:
        # Обязательные политики:
        db: str
        table: str
        columns: 
            ExportColName1: TableColName1 UInt64
            ExportColName2: TableColName2 DateTime DEFAULT now()
        orders: List[str]
        # Опциональные политики:
        partition: Optional[Union[List[str], str]] = None

    # Обязательные политики:
    data_cleaning_mode: Literal["partition", "off", "truncate"]
    # Опциональные политики:
    # Скрипт SQL, который выполниться после вставки данных.
    sql_after: Optional[list[str]] = None
    # Скрипт SQL, который выполниться перед экспортом данных.
    sql_before: Optional[list[str]] = None
    # Максимальное кол-во одновременных запущенных вставок данных.
    concurrency: int = 2
    # Ограничивает выполнение в случае переполнения доступных слотов хотя бы в одном из указанных пулов.
    pools: Optional[list[str]] = None
```


### Схемы колонок таблицы (load.table_schema.columns)

Колонки прописываются в паре с колонками экспорта, 
это позволяет определить в какой тип нужно преобразовать данные.
Хотя для каждого столбца есть возможность указать параметры 
преобразования вручную в **transform.column_schema**.

```yaml
load:
    table_schema:
        columns:
            ExportColName1: TableColName1 UInt64
            ExportColName2: TableColName2 DateTime DEFAULT now()
```


### Политика обновления устаревших данных (load.data_cleaning_mode)

- **partition** - будет удалять данные партиций перед вставкой, 
  которые есть в загружаемых данных. Обязательно заполнить table_schema.partition

  На данный момент скрипт поддерживает обновление устаревших данных
  за предыдущие дни только через удаление партиций.
  Поэтому в партиции должен присутствовать столбец с типом Date.

  Логика удаления партиций такая. 
  Скрипт берет уникальные значения экспортируемого датасета в колонках из table_schema.partition
  и отправляет запрос на удаление этих партиций.

- **truncate** - будет очищать таблицу перед вставкой данных

- **off** - не будет проводить операции перед вставкой данных


### Логика вставки данных

1. Скрипт перед началом экспорта данных создает промежуточную таблицу, 
в которую вставляет экспортируемые данные.
2. После завершения, скрипт удаляет данные в целевой таблице 
согласно политики data_cleaning_mode
3. далее перемещает данные из промежуточной таблицы в целевую таблицу, 
4. после удаляет промежуточную таблицу.


## Политика преобразования данных (transform)

```yaml
transform:
    # Required:
    error_policy: Literal["raise", "default", "coerce", "ignore"]
    # Optional:
    timezone: Optional[str] = None
    column_schema:
        ExportColName: # Название колонки, которое экспортируется.
            errors: Optional[Literal["raise", "default", "coerce", "ignore"]] = None
            dt_format: Optional[str] = None
            null_values: Optional[list] = None
            clear_values: Optional[list] = None
    concurrency: int = 100
    pools: Optional[list[str]] = None
```


### Политика поведения при возникновении ошибки в преобразовании данных (transform.error_policy)

- **raise** - при ошибке преобразования вызовет Exception
- **default** - при ошибке преобразования поставит значение по умолчанию, 
  в зависимости от типа столбца
- **ignore** - при ошибке преобразования, проигнорирует. 
  Но вероятно при вставке данных, процесс развалиться
- **coerce** - при ошибке поставит null значение


### Политики преобразования колонки (transform.column_schema)

Здесь указываются параметры преобразования для колонки.

- **errors** - значения, как и в error_policy

- **dt_format** - формат даты для десериализации из строки

- **null_values** - указанные значения будет воспринимать, 
  как null и преобразует в него. Например если указать значение "--", 
  то если встретит его, преобразует в null.

- **clear_values** - очищает указанные значение. 
  Если столбец Nullable, то преобразует в null, если нет, 
  то в значение по умолчанию в зависимости от типа столбца.
