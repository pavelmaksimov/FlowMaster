# CLI

## Config
Команды для конфигураций.

```shell
flowmaster config --help
```

Список названий файлов конфигураций.
```shell
flowmaster config list
```

Валидация конфигураций.
```shell
flowmaster config validate
```

Выведет конфигурации с количеством невыполненных потоков.
```shell
flowmaster config errors
```


## Item
Команды для записей работ потоков.

```shell
flowmaster item --help
```

Выведет 20 последних записей выполненных потоков.
```shell
flowmaster item list {config_filename} --limit 20
```
Выведет записи не выполненных потоков.
```shell
flowmaster item list-errors {config_filename}
```

Изменяет состояния записей потоков, чтобы они перезапустились.
```shell
flowmaster item restart {config_filename} -s 2021-01-01 --end 2021-01-31
```

Изменяет состояния записей не выполненных потоков, чтобы они перезапустились.
```shell
flowmaster item restart-errors {config_filename}
```

Удаляет записи потока.
```shell
flowmaster item clear {config_filename}
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
