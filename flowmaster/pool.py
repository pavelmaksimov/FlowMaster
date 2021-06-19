import datetime as dt
from collections import defaultdict
from typing import Literal

from flowmaster.setttings import Settings
from flowmaster.utils.logging_helper import logger
from flowmaster.utils.yaml_helper import YamlHelper


class Counter:
    def __init__(self):
        self.counters = defaultdict(int)

    def __setitem__(self, tag, switch: Literal[1, -1]):
        if switch not in (1, -1):
            logger.warning("[{}] disabled counter switch: {}", tag, switch)

        if switch == -1 and self.counters[tag] == 0:
            logger.warning(
                "[{}] Attempting to decrease the counter to less than zero", tag
            )
        else:
            self.counters[tag] += switch

    def __getitem__(self, item):
        return self.counters[item]


class Pool:
    def __init__(self, pools: dict[str, int]):
        self.limits = {}
        self.sizes = {}
        self.append_pools(pools)

    def append_pools(self, pools: dict[str, int]) -> None:
        for tag, limit in pools.items():
            tag = self._get_uniq_tagname(tag)
            self.limits[tag] = limit
            self.sizes[tag] = Counter()

    def update_pools(self, pools: dict[str, int]) -> None:
        for tag, limit in pools.items():
            self.limits[tag] = limit
            if tag not in self.sizes:
                self.sizes[tag] = Counter()

    def allow(self, tags: list[str]) -> bool:
        results = []
        for tag in tags:
            is_free = self.sizes[tag][tag] < self.limits[tag]
            results.append(is_free)
            if is_free is False:
                logger.debug(f"Pool '{tag}' full")

        return all(results)

    def _get_uniq_tagname(self, tag: str) -> str:
        if tag in self.limits:
            tag += "_"
            return self._get_uniq_tagname(tag)
        return tag

    def __setitem__(self, tags: list[str], switch: Literal[1, -1]) -> None:
        for tag in tags:
            counter = self.sizes[tag]
            counter[tag] = switch

    def __getitem__(self, tag: str) -> int:
        counter: Counter = self.sizes[tag]
        return counter[tag]

    def info(self) -> list[dict]:
        data = []
        for tag, limit in self.limits.items():
            data.append(
                {
                    "name": tag,
                    "size": self[tag],
                    "limit": limit,
                    "datetime": dt.datetime.now(),
                }
            )

        return data

    def __str__(self) -> str:
        text = ""
        for tag, limit in self.limits.items():
            text += f"{tag}: {self[tag]}, limit={limit}\n"

        return str(text)


pools_dict = YamlHelper.parse_file(str(Settings.POOL_CONFIG_FILEPATH))
pools = Pool(pools_dict)
