import time
from pathlib import Path
from typing import Iterator

from flowmaster.enums import Statuses
from flowmaster.executors import ExecutorIterationTask, SleepIteration
from flowmaster.operators.base.policy import BaseNotebook
from flowmaster.operators.base.work import Work
from flowmaster.utils.logging_helper import getLogger, create_logfile
from flowmaster.utils.notifications import send_codex_telegram_message


class BaseOperator:
    name = "base"

    def __init__(self, notebook: BaseNotebook):
        from flowmaster.models import FlowItem

        self.notebook = notebook

        self.logger = getLogger()
        self.Work = Work(notebook, self.logger)
        self.Model = FlowItem  # TODO replace to Pydantic

        if self.notebook.work is not None:
            self.concurrency_pool_names = notebook.work.pools or []
            if self.notebook.work.concurrency is not None:
                self.concurrency_pool_names.append(f"__{self.name}_concurrency__")
                self.add_pool(
                    f"__{self.name}_concurrency__", self.notebook.work.concurrency
                )

    def add_pool(self, name: str, limit: int) -> None:
        from flowmaster.pool import pools

        pools.update_pools({name: limit})

    def get_logfile_path(self) -> Path:
        worktime = self.Work.current_worktime.strftime("%Y-%m-%dT%H-%M-%S")
        worktime = worktime.replace("T00-00-00", "")
        return create_logfile(f"{worktime}.log", self.notebook.name)

    def add_logger_file(self, dry_run: bool) -> None:
        logfile_path = self.get_logfile_path()
        level = "DEBUG" if dry_run else "INFO"
        self.logger.add(
            logfile_path,
            level=level,
            colorize=True,
            backtrace=True,
            diagnose=True,
            retention=self.Work.interval_timedelta * 90,
            encoding="utf8",
        )
        error_logfile_path = create_logfile(f"errors.log", self.notebook.name)
        self.logger.add(
            error_logfile_path,
            level="ERROR",
            colorize=True,
            backtrace=True,
            diagnose=True,
            rotation="10 MB",
            encoding="utf8",
        )

    def send_notifications(self, status: Statuses.LiteralT, **kwargs) -> None:
        if self.notebook.work.notifications is None:
            return

        message = f"{self.notebook.name} {status}"
        for key, value in kwargs.items():
            message += f"\n{key}: {value}"

        codex_tg = self.notebook.work.notifications.codex_telegram

        if status == Statuses.success:
            if codex_tg.on_success:
                send_codex_telegram_message(codex_tg.links, message)

        elif status in Statuses.error_statuses:
            if not self.Model.allow_execute_flow(
                self.notebook.name,
                notebook_hash=self.notebook.hash,
                max_fatal_errors=self.notebook.work.max_fatal_errors,
            ) or not self.Model.retry_error_items(
                self.notebook.name, self.Work.retries, retry_delay=0
            ):
                if codex_tg.on_failure:
                    send_codex_telegram_message(codex_tg.links, message)
            else:
                if codex_tg.on_retry:
                    send_codex_telegram_message(codex_tg.links, message)

    def __call__(self, *args, dry_run: bool = False, **kwargs) -> Iterator:
        """Operator script."""
        raise NotImplementedError()

    def _iterator(self, *args, dry_run: bool = False, **kwargs) -> Iterator:
        """Operator script wrapper."""
        self.logger.info("Start flow: {}", self.notebook.name)
        yield from self(*args, dry_run=dry_run, **kwargs)
        self.logger.info("End flow: {}", self.notebook.name)

    def task(self, *args, **kwargs) -> ExecutorIterationTask:
        """Flow wrapper for scheduler and executor."""
        return ExecutorIterationTask(
            self._iterator(*args, **kwargs),
            expires=self.Work.expires,
            soft_time_limit_seconds=self.Work.soft_time_limit_seconds,
        )

    def dry_run(self, *args, **kwargs) -> None:
        """Immediate execution without restrictions."""
        kwargs["dry_run"] = True
        for item in self(*args, **kwargs):
            if isinstance(item, SleepIteration):
                time.sleep(item.rest_of_sleep())
