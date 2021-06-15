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
    from flowmaster.operators.etl.policy import ETLNotebook


class FakeDataExport(ExportAbstract):
    def __init__(self, notebook: "ETLNotebook", logger: Optional[Logger] = None):
        self.rows = notebook.export.rows
        self.columns = notebook.export.columns
        super(FakeDataExport, self).__init__(notebook, logger)

    def __call__(self, *args, **kwargs) -> Iterator[ExportContext]:
        self.logger.info("Exportation data")

        fake_data = [
            [getattr(fake, col)() for col in self.columns] for _ in range(self.rows)
        ]

        for data in chunker(fake_data, (int(self.rows / 5) or 1)):
            yield ExportContext(
                export_kwargs={},
                columns=self.columns,
                data=data,
                data_orient=DataOrient.values,
            )
