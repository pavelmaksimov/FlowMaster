from collections import defaultdict
from typing import Literal

from flowmaster.setttings import POOL_CONFIG_FILEPATH
from flowmaster.utils.logging_helper import CreateLogger
from flowmaster.utils.yaml_helper import YamlHelper

logger = CreateLogger("counter-pool", filename="counter_pool.log")


class Counter:
    def __init__(self):
        self.counters = defaultdict(int)

    def __setitem__(self, tag, switch: Literal[1, -1]):
        if switch not in (1, -1):
            logger.warning("[%s] disabled counter switch: %s", tag, switch)

        if switch == -1 and self.counters[tag] == 0:
            logger.warning(
                "[%s] Attempting to decrease the counter to less than zero", tag
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

    def append_pools(self, pools: dict[str, int]):
        for tag, limit in pools.items():
            tag = self._get_uniq_tagname(tag)
            self.limits[tag] = limit
            self.sizes[tag] = Counter()

    def update_pools(self, pools: dict[str, int]):
        for tag, limit in pools.items():
            self.limits[tag] = limit
            if tag not in self.sizes:
                self.sizes[tag] = Counter()

    def allow(self, tags: list[str]):
        return all([self.sizes[tag][tag] < self.limits[tag] for tag in tags])

    def _get_uniq_tagname(self, tag: str):
        if tag in self.limits:
            tag += "_"
            return self._get_uniq_tagname(tag)
        return tag

    def __setitem__(self, tags: list[str], switch: Literal[1, -1]):
        for tag in tags:
            counter = self.sizes[tag]
            counter[tag] = switch

    def __getitem__(self, tag: str):
        counter = self.sizes[tag]
        return counter[tag]

    def __str__(self):
        text = ""
        for tag, limit in self.limits.items():
            text += f"{tag}: {self[tag]}, limit={limit}\n"

        return str(text)


pools_dict = YamlHelper.parse_file(str(POOL_CONFIG_FILEPATH))
pools = Pool(pools_dict)
