from typing import Optional

from pydantic import BaseModel

from flowmaster.operators.base.policy import BasePolicy


class GoogleSheetsExportPolicy(BasePolicy):
    class Credentials(BaseModel):
        service_account_file: str

    credentials: Credentials
    sheet_id: str
    columns: list[str]
    with_columns: bool
    page_id: int = 0
    start: Optional[tuple[int, int]] = None
    end: Optional[tuple[int, int]] = None
