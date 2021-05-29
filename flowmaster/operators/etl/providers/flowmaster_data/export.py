from typing import TYPE_CHECKING, Iterator

import peewee
import pendulum

from flowmaster.models import FlowItem
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.types import DataOrient

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLFlowConfig
    from flowmaster.operators.etl.providers.flowmaster_data.policy import (
        FlowmasterDataExportPolicy,
    )


class FlowmasterDataExport(ExportAbstract):
    def __init__(self, config: "ETLFlowConfig", *args, **kwargs):
        super(FlowmasterDataExport, self).__init__(config, *args, **kwargs)

    def resource_items(self, start_period, end_period, **kwargs):
        query: peewee.ModelSelect = FlowItem.select()

        if self.export.export_mode == "by_date":
            query = query.where(
                FlowItem.worktime >= start_period, FlowItem.worktime <= end_period
            )

        yield ExportContext(
            export_kwargs={},
            columns=self.export.columns,
            data=list(query.dicts()),
            data_orient=DataOrient.dict,
        )

    def resource_pools(self):
        from flowmaster.pool import pools

        yield ExportContext(
            export_kwargs={},
            columns=self.export.columns,
            data=pools.info(),
            data_orient=DataOrient.dict,
        )

    def resource_queues(self):
        from flowmaster.utils.thread_executor import task_queue, sleeptask_queue

        data = [
            {
                "name": "tasks",
                "size": task_queue.qsize(),
                "datetime": pendulum.now(),
            },
            {
                "name": "sleeptasks",
                "size": sleeptask_queue.qsize(),
                "datetime": pendulum.now(),
            },
        ]

        yield ExportContext(
            export_kwargs={},
            columns=self.export.columns,
            data=data,
            data_orient=DataOrient.dict,
        )

    def __call__(self, start_period, end_period, **kwargs) -> Iterator[ExportContext]:
        self.logger.info("Exportation data")

        self.export: "FlowmasterDataExportPolicy" = self.model_templating(
            start_period, end_period, model=self.config.export
        )

        if self.export.resource == "items":
            yield from self.resource_items(start_period, end_period, **kwargs)

        elif self.export.resource == "queues":
            yield from self.resource_queues()

        elif self.export.resource == "pools":
            yield from self.resource_pools()
