import contextlib
import os
import shutil
from pathlib import Path
from typing import Iterator, Optional

import yaml
from pydantic import ValidationError, BaseModel

from flowmaster.models import FlowItem
from flowmaster.operators.etl.policy import ETLNotebookPolicy
from flowmaster.setttings import Settings


def archive_notebook(name: str) -> None:
    shutil.move(Settings.NOTEBOOKS_DIR / name, Settings.ARCHIVE_NOTEBOOKS_DIR / name)


def unarchive_notebook(name: str) -> None:
    shutil.move(Settings.ARCHIVE_NOTEBOOKS_DIR / name, Settings.NOTEBOOKS_DIR / name)


def iter_active_notebook_filenames() -> Iterator[str]:
    file_names = os.listdir(Settings.NOTEBOOKS_DIR)
    for file_name in file_names:
        if file_name.endswith(".yml") or file_name.endswith(".yaml"):
            yield file_name


def iter_archive_notebook_filenames() -> Iterator[str]:
    file_names = os.listdir(Settings.ARCHIVE_NOTEBOOKS_DIR)
    for file_name in file_names:
        if file_name.endswith(".yml") or file_name.endswith(".yaml"):
            yield file_name


def get_filepath_notebook(name: str) -> Optional[Path]:
    filenames = os.listdir(Settings.NOTEBOOKS_DIR)
    if name in filenames:
        return Settings.NOTEBOOKS_DIR / name

    archive_filenames = os.listdir(Settings.ARCHIVE_NOTEBOOKS_DIR)
    if name in archive_filenames:
        return Settings.ARCHIVE_NOTEBOOKS_DIR / name


def is_archive_notebook(name: str) -> bool:
    archive_filenames = os.listdir(Settings.ARCHIVE_NOTEBOOKS_DIR)
    filenames = os.listdir(Settings.NOTEBOOKS_DIR)

    if name in filenames:
        return False

    if name in archive_filenames:
        return True


def read_notebook_file(name: str) -> Optional[str]:
    with contextlib.suppress(FileNotFoundError):
        with open(Settings.NOTEBOOKS_DIR / name, "r") as f:
            return f.read()

    with contextlib.suppress(FileNotFoundError):
        with open(Settings.ARCHIVE_NOTEBOOKS_DIR / name, "r") as f:
            return f.read()

    raise FileNotFoundError(name)


def parse_and_validate_notebook_yaml(text: str) -> tuple[bool, dict, str]:
    try:
        data = yaml.safe_load(text)
    except yaml.error.YAMLError as exc:
        return False, {}, str(exc)
    else:
        return True, data, ""


def validate_notebook_policy(
    name: str, notebook_dict: dict
) -> tuple[bool, dict, Optional[BaseModel], str]:
    try:
        if ".etl" in name:
            notebook_class = ETLNotebookPolicy
        else:
            raise NotImplementedError()

        if "name" not in notebook_dict:
            notebook_dict["name"] = name

        policy = notebook_class(**notebook_dict)

    except ValidationError as exc:
        return False, notebook_dict, None, str(exc)

    else:
        return True, notebook_dict, policy, ""


def get_notebook(name: str) -> tuple[bool, str, dict, Optional[BaseModel], str]:
    text = read_notebook_file(name)
    validate_text, notebook_dict, error = parse_and_validate_notebook_yaml(text)
    if validate_text:
        validate_policy, notebook_dict, policy, error = validate_notebook_policy(
            name, notebook_dict
        )
        return validate_policy, text, notebook_dict, policy, error
    else:
        return validate_text, text, notebook_dict, None, error


def save_notebook(name: str, text: str, is_archive: bool) -> None:
    if is_archive:
        path = Settings.ARCHIVE_NOTEBOOKS_DIR / name
    else:
        path = Settings.NOTEBOOKS_DIR / name

    with open(path, "w") as f:
        f.write(text)


def save_new_notebook(name: str, text: str, is_archive: bool) -> Optional[bool]:
    filenames = os.listdir(Settings.NOTEBOOKS_DIR) + os.listdir(
        Settings.ARCHIVE_NOTEBOOKS_DIR
    )
    if name in filenames:
        return False

    return save_notebook(name, text, is_archive)


def delete_notebook(name: str) -> None:
    path = get_filepath_notebook(name)
    if path:
        path.unlink()

    FlowItem.clear(name)
