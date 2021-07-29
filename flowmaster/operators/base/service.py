from flowmaster.models import FlowItem, FlowStatus
from flowmaster.operators.base.policy import Notebook
from flowmaster.operators.base.work import Work
from flowmaster.utils.logging_helper import getLogger, create_logfile
from flowmaster.utils.notifications import send_codex_telegram_message


class BaseOperator:
    def __init__(self, notebook: Notebook):
        self.notebook = notebook
        self.name = notebook.name

        self.logger = getLogger()
        self.Work = Work(notebook, self.logger)
        self.Model = FlowItem  # TODO replace to Pydantic

    def get_logfile_path(self):
        worktime = self.Work.current_worktime.strftime("%Y-%m-%dT%H-%M-%S")
        worktime = worktime.replace("T00-00-00", "")
        return create_logfile(f"{worktime}.log", self.name)

    def add_logger_file(self, dry_run):
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
        error_logfile_path = create_logfile(f"errors.log", self.name)
        self.logger.add(
            error_logfile_path,
            level="ERROR",
            colorize=True,
            backtrace=True,
            diagnose=True,
            rotation="10 MB",
            encoding="utf8",
        )

    def send_notifications(self, status: FlowStatus.LiteralT, **kwargs):
        if self.notebook.work.notifications is None:
            return

        message = f"{self.name} {status}"
        for key, value in kwargs.items():
            message += f"\n{key}: {value}"

        codex_tg = self.notebook.work.notifications.codex_telegram

        if status == FlowStatus.success:
            if codex_tg.on_success:
                send_codex_telegram_message(codex_tg.links, message)

        elif status in FlowStatus.error_statuses:
            if not self.Model.allow_execute_flow(
                self.name,
                notebook_hash=self.notebook.hash,
                max_fatal_errors=self.notebook.work.max_fatal_errors,
            ) or not self.Model.retry_error_items(
                self.name, self.Work.retries, retry_delay=0
            ):
                if codex_tg.on_failure:
                    send_codex_telegram_message(codex_tg.links, message)
            else:
                if codex_tg.on_retry:
                    send_codex_telegram_message(codex_tg.links, message)
