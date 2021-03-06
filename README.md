![logo](docs/img/logoza.ru.png)

# ETL framework based on Yaml configs in Python

![Supported Python Versions](https://img.shields.io/static/v1?label=python&message=>=3.9&color=blue)
![License](https://img.shields.io/static/v1?label=license&message=GPLv3&color=green)
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>

A light framework for creating data streams. 
Setting up streams through configuration in the Yaml file.
There is a schedule, task pools, concurrency limitation.
Works quickly, does not require a lot of resources. 
Runs on Windows and Linux.
Flow run in parallel via threading library. 
Internally SQLite Database.
Native data transformation.
There is a web interface.

At the moment there are connectors to sources
- CSV file
- SQLite
- Postgres 
- MySQL
- Yandex Metrika Management API
- Yandex Metrika Stats API
- Yandex Metrika Logs API
- Yandex Direct API
- Yandex Direct Report API
- Criteo
- Google Sheets

Storages
- Save to csv file
- Clickhouse


## [Documentation](docs/main.md)

## Requirements
- python >=3.9
- virtual environment


## Settings

**It is highly recommended to install in a virtual environment.**

Flowmaster needs a home, '{HOME}/FlowMaster' is the default,\
but you can lay foundation somewhere else if you prefer\
(optional)

For Windows

    setx FLOWMASTER_HOME "{YOUR_PATH}"

For Linux

    export FLOWMASTER_HOME={YOUR_PATH}

## Installing
    pip install flowmaster==0.7.1

    # For install web UI.
    pip install flowmaster[webui]==0.7.1

    # Optional libraries.
    pip install flowmaster[clickhouse,postgres,mysql,yandexdirect,yandexmetrika,criteo,googlesheets]==0.7.1

## Run
    flowmaster run --help
    flowmaster run

## WEB UI
http://localhost:8822

## [CHANGELOG](CHANGELOG.md)


## Support

[Telegram support chat](https://t.me/joinchat/DhWJYG_yECYyYTEy)


## Author
Pavel Maksimov

My contacts
[Telegram](https://t.me/pavel_maksimow),
[Facebook](https://www.facebook.com/pavel.maksimow)

Удачи тебе, друг! Поставь звездочку ;)
