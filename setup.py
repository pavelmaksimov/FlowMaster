#!/usr/bin/env python
import os
import re

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    readme = fh.read()

package = "flowmaster"


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    init_py = open(os.path.join(package, "__init__.py")).read()
    return re.search("^__version__ = ['\"]([^'\"]+)['\"]", init_py, re.MULTILINE).group(
        1
    )


setup(
    name="FlowMaster",
    version=get_version(package),
    description="ETL flow framework based on Yaml configs in Python",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Pavel Maksimov",
    author_email="vur21@ya.ru",
    url="https://github.com/pavelmaksimov/flowmaster",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "requests",
        "pytz",
        "clickhousepy==2021.3.10",
        "DataGun>=0.1.0, <0.2.0",
        "orjson>=3.0.0, <4.0.0",
        "Faker>=5.0.0, <9.0.0",
        "peewee>=3.0.0, <4.0.0",
        "pendulum>=2.0.0, <3.0.0",
        "pydantic>=1.0.0, <2.0.0",
        "PyYAML>=5.0.0, <6.0.0",
        "tapi-yandex-metrika==2021.2.21",
        "tapi-yandex-direct==2021.5.3",
        "typer>=0.3.0, <0.4.0",
        "Jinja2>=2.0.0, <3.0.0",
        "loguru>=0.5, <0.6",
    ],
    license="GPLv3",
    keywords="etl,flowmaster,flow,airflow,prefect,schedule,scheduler,tasker",
    test_suite="tests",
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "flowmaster=flowmaster.__main__:app",
        ],
    },
)
