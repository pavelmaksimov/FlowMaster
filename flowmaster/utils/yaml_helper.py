import os
from pathlib import PurePath
from typing import Iterator, Union

import yaml


class YamlHelper:
    @classmethod
    def parse_file(self, path: Union[str, PurePath]) -> dict:
        if isinstance(path, PurePath):
            path = str(path)

        with open(path, "rb") as f:
            config = yaml.full_load(f.read())
            return config

    @classmethod
    def iter_parse_file_from_dir(
        cls, dirpath: str, match: str = None
    ) -> Iterator[tuple[str, dict]]:
        file_name_list = os.listdir(dirpath)
        for file_name in file_name_list:
            if file_name.endswith(".yml") or file_name.endswith(".yaml"):
                if match is None or match in file_name:
                    yield file_name, cls.parse_file(os.path.join(dirpath, file_name))
