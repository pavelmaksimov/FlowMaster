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

        if status == FlowStatus.success:
            codex_tg = self.notebook.work.notifications.codex_telegram

            if codex_tg.on_success:
                send_codex_telegram_message(codex_tg.links, message)

            if codex_tg.on_retry:
                send_codex_telegram_message(codex_tg.links, message)
