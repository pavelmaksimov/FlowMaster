import importlib
import os
import pkgutil
from types import ModuleType
from typing import Iterator, Union, Any


def iter_all_modules(
    package: Union[str, ModuleType],
    prefix: str = "",
) -> Iterator[str]:
    if isinstance(package, str):
        path = package
    else:
        # Type ignored because typeshed doesn't define ModuleType.__path__
        # (only defined on packages).
        package_path = package.__path__  # type: ignore[attr-defined]
        path, prefix = package_path[0], package.__name__ + "."

    for _, name, is_package in pkgutil.iter_modules([path]):
        if is_package:
            yield prefix + name

            for m in iter_all_modules(os.path.join(path, name), prefix=name + "."):
                yield prefix + m
        else:
            yield prefix + name


def iter_module_objects(packet: Union[str, ModuleType]) -> Any:
    for import_path in iter_all_modules(packet):
        module = importlib.import_module(import_path)

        for object_name in dir(module):
            if not object_name.startswith("_"):
                yield getattr(module, object_name)
