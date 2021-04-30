# Roadmap

### ETL
#### Добавить источники данных

- Яндекс Маркет (in developing)
- Яндекс Директ (in developing)
- Google Analytics (in developing)
- Google Adwords (in developing)
- Mytarget
- Criteo
- File
- Clickhouse
- BigQuery
- Postgres
- Mysql
- SQlite

#### Добавить хранилища

- Postgres (in developing)
- MySQL (in developing)
- Google BigQuery (in developing)
- Google Sheets (in developing)

### Общее
- Оператор скрипта Python
- Оператор скрипта SQL
- Запуск потока после другого потока
- Ограничение времени исполнения потока
- Сохранение отброшенных невалидных строк на этапа transform в ETL потоке, 
  для последующий ручного редактирования и повторной вставки
- Создание конфигурации потока через форму в браузере или desktop приложении
- Добавить поддержку Sentry
- Оповещение по почте
  
### UI
- Остановка, перезапуск потоков
- Форма создания конфигураций потоков

### Новые параметры конфигов потоков

- Возможность указать в одном конфиге несколько аккаунтов в секции экспорта
