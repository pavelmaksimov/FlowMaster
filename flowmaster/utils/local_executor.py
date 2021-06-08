import logging
import time

from flowmaster.operators.base.work import ordering_flow_tasks
from flowmaster.utils.logging_helper import CreateLogger


def sync_executor(*, interval: int = 20, orders: int = None, dry_run: bool = False):
    logger = CreateLogger("local_executor", "local_executor.log", level=logging.INFO)
    begin = time.time()
    duration = interval
    count_orders = 0

    while True:
        logger.info("Ordering flow tasks")

        for task in ordering_flow_tasks(logger=logger, dry_run=dry_run):
            list(task)

        if duration >= interval:
            duration = 0
            begin = time.time()
        else:
            time.sleep(interval - duration)
            duration += time.time() - begin

        count_orders += 1
        if orders and count_orders >= orders:
            break
