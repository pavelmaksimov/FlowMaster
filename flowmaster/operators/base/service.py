from flowmaster.models import FlowItem, FlowStatus
from flowmaster.operators.base.policy import Notebook
from flowmaster.operators.base.work import Work
from flowmaster.utils.logging_helper import getLogger
from flowmaster.utils.notifications import send_codex_telegram_message


class BaseOperator:
    def __init__(self, notebook: Notebook):
        self.notebook = notebook
        self.name = notebook.name

        self.logger = getLogger()
        self.Work = Work(notebook, self.logger)
        self.Model = FlowItem  # TODO replace to Pydantic

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
