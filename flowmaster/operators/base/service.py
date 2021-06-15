import logging

from flowmaster.models import FlowItem, FlowStatus
from flowmaster.operators.base.work import Work
from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.utils.logging_helper import CreateLogger
from flowmaster.utils.notifications import send_codex_telegram_message


class BaseOperator:
    def __init__(self, notebook: ETLNotebook, loglevel: int = logging.INFO):
        self.loglevel = loglevel
        self.notebook = notebook
        self.name = notebook.name

        self.logger = CreateLogger(self.name, level=logging.INFO)
        self.Work = Work(notebook, self.logger)
        self.Model = FlowItem  # TODO replace to Pydantic

    def send_notifications(self, status: FlowStatus.LiteralT, **kwargs):
        message = f"{self.name} {status}"

        for key, value in kwargs.items():
            message += f"\n{key}: {value}"

        if self.notebook.work.notifications is None:
            return

        if status == FlowStatus.success:
            codex_tg = self.notebook.work.notifications.codex_telegram

            if codex_tg.on_success:
                send_codex_telegram_message(codex_tg.links, message)

            if codex_tg.on_retry:
                send_codex_telegram_message(codex_tg.links, message)
