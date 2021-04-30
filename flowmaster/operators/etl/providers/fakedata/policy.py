from typing import Literal

from flowmaster.operators.base.policy import BasePolicy


class FakeDataExportPolicy(BasePolicy):
    rows: int
    columns: list[Literal["name", "phone_number", "date_time", "company"]] = [
        "name",
        "phone_number",
        "date_time",
        "company",
    ]
    concurrency: int = 10
