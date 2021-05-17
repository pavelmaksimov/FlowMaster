import csv
from typing import TYPE_CHECKING, Iterator

from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.types import DataOrient

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLFlowConfig
    from flowmaster.operators.etl.providers.csv.policy import CSVExportPolicy


class CSVExport(ExportAbstract):
    def __init__(self, config: "ETLFlowConfig", *args, **kwargs):
        self.export: "CSVExportPolicy" = config.export
        super(CSVExport, self).__init__(config, *args, **kwargs)

    def chunker(self, iterator: Iterator, chunk_size: int) -> Iterator[list[dict]]:
        while True:
            data = []
            try:
                while len(data) < chunk_size:
                    row = next(iterator)
                    data.append(row)

            except StopIteration:
                break

            finally:
                if data:
                    yield data

    def __call__(self, *args, **kwargs) -> Iterator[ExportContext]:
        self.logger.info("Exportation data")

        params = self.collect_params(*args, **self.export.dict())

        with open(
            params["file_path"],
            mode="r",
            newline=self.export.newline,
            encoding=self.export.encoding,
        ) as file:
            if self.export.with_columns is None:
                data_orient = DataOrient.values
                row_iterator = csv.reader(file, delimiter=self.export.sep)
            else:
                data_orient = DataOrient.dict
                row_iterator = csv.DictReader(
                    file, fieldnames=self.export.columns, delimiter=self.export.sep
                )

            # Skip begin lines.
            for _ in range(self.export.skip_begin_lines):
                try:
                    next(row_iterator)
                except StopIteration:
                    ...

            # Skip columns.
            if self.export.with_columns:
                next(row_iterator)

            for chunk in self.chunker(
                row_iterator,
                chunk_size=self.export.chunk_size,
            ):
                yield ExportContext(
                    export_kwargs={},
                    columns=self.export.columns,
                    data=chunk,
                    data_orient=data_orient,
                )
