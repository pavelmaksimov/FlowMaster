#!/usr/bin/env python
import os
import re

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    readme = fh.read()

with open("requirements.in", "r") as f:
    requirements = f.read().splitlines()

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
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    license="GPLv3",
    keywords="etl,flowmaster,flow,airflow,prefect,schedule,scheduler,tasker",
    test_suite="tests",
    python_requires='>=3.9',
    entry_points={
        'console_scripts': [
            'flowmaster=flowmaster.__main__:app',
        ],
    },
)
