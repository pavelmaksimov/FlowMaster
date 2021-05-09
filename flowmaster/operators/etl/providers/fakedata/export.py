from logging import Logger
from typing import TYPE_CHECKING, Optional, Iterator

from faker import Faker

from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.types import DataOrient
from flowmaster.utils import chunker

fake = Faker()
fake.seed_instance(0)

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig


class FakeDataExport(ExportAbstract):

    def __init__(self, config: "ETLFlowConfig", logger: Optional[Logger] = None):
        self.rows = config.export.rows
        self.columns = config.export.columns
        super(FakeDataExport, self).__init__(config, logger)

    def __call__(self, *args, **kwargs) -> Iterator[ExportContext]:
        self.logger.info("Exportation data")

        fake_data = [
            [getattr(fake, col)() for col in self.columns] for _ in range(self.rows)
        ]

        for data in chunker(fake_data, (int(self.rows / 5) or 1)):
            yield ExportContext(export_kwargs={}, columns=self.columns, data=data, data_orient=DataOrient.values)
