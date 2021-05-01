#!/usr/bin/env python
import os
import re

from setuptools import setup

with open("README.md", "r") as fh:
    readme = fh.read()

package = "flowmaster"


def get_description():
    return re.match(r"^# (.+)\n", readme).group(1)


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
    description=get_description(),
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Pavel Maksimov",
    author_email="vur21@ya.ru",
    url="https://github.com/pavelmaksimov/flowmaster",
    packages=[package],
    include_package_data=True,
    install_requires=[
        "requests",
        "DataGun>=0.1.0, <0.2.0",
        "orjson>=3.0.0, <4.0.0",
        "Faker>=5.0.0, <9.0.0",
        "peewee>=3.0.0, <4.0.0",
        "pendulum>=2.0.0, <3.0.0",
        "pydantic>=1.0.0, <2.0.0",
        "PyYAML>=5.0.0, <6.0.0",
        "tapi-yandex-metrika==2021.2.21",
        "typer>=0.3.0, <0.4.0",
        "Jinja2>=2.0.0, <3.0.0",
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
