from typing import Literal, Union

from pydantic import PositiveInt

from flowmaster.operators.base.policy import BasePolicy


class FlowmasterDataExportPolicy(BasePolicy):
    resource: Literal["items", "pools", "queues"]
    columns: list[
        Union[
            Literal[
                "name",
                "worktime",
                "operator",
                "status",
                "data",
                "notebook_hash",
                "retries",
                "duration",
                "info",
                "started_utc",
                "finished_utc",
                "created_utc",
                "updated",
            ],
            # pools
            Literal["name", "size", "limit", "datetime"],
            # queues
            Literal["name", "size", "datetime"],
        ]
    ]
    export_mode: Literal["all", "by_date"] = "all"
    concurrency: PositiveInt = 1
