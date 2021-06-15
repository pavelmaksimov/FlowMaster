# Документация

- [Как создать конфигурацию потока данных](notebooks/etl/base.md)
- [Как добавить свой скрипт экспорта данных](plugins.md)
- [Интерфейс командной строки](cli.md) (CLI)

После установки инструмента, 
для создания потока данных необходимо сформировать конфигурацию.

Проверить конфигурации

    flowmaster notebook validate

Далее запустить инструмент

    flowmaster run


Логи сохраняются в `FlowMaster/logs`\
Сейчас в логировании есть проблемы, иногда логи пишутся в один файл.

Мониторинг

    flowmaster item list {notebook_filename} --limit 20

Перезапуск потока

    flowmaster item restart {notebook_filename} -s 2021-01-01 --end 2021-01-31
    # or
    flowmaster item restart-errors {notebook_filename}
