# CLI

## Notebook
Команды для конфигураций.

```shell
flowmaster notebook --help
```

Список названий файлов конфигураций.
```shell
flowmaster notebook list
```

Валидация конфигураций.
```shell
flowmaster notebook validate
```

Выведет конфигурации с количеством невыполненных потоков.
```shell
flowmaster notebook errors
```


## Item
Команды для записей работ потоков.

```shell
flowmaster item --help
```

Выведет 20 последних записей выполненных потоков.
```shell
flowmaster item list {notebook_filename} --limit 20
```
Выведет записи не выполненных потоков.
```shell
flowmaster item list-errors {notebook_filename}
```

Изменяет состояния записей потоков, чтобы они перезапустились.
```shell
flowmaster item restart {notebook_filename} -s 2021-01-01 --end 2021-01-31
```

Изменяет состояния записей не выполненных потоков, чтобы они перезапустились.
```shell
flowmaster item restart-errors {notebook_filename}
```

Удаляет записи потока.
```shell
flowmaster item clear {notebook_filename}
```


## DB
Команды для базы данных

```shell
flowmaster db --help
```

Приведет базу данных в первоначальное состояние
```shell
flowmaster db reset
```
