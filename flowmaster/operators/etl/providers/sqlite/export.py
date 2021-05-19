from typing import TYPE_CHECKING, Iterator

import peewee

from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.operators.etl.types import DataOrient
from flowmaster.utils import chunker

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLFlowConfig
    from flowmaster.operators.etl.providers.sqlite.policy import SQLiteExportPolicy


class SQLiteExport(ExportAbstract):
    def __init__(self, config: "ETLFlowConfig", *args, **kwargs):
        super(SQLiteExport, self).__init__(config, *args, **kwargs)

    def __call__(self, *args, **kwargs) -> Iterator[ExportContext]:
        self.logger.info("Exportation data")

        self.export: "SQLiteExportPolicy" = self.model_templating(*args, model=self.config.export)

        db = peewee.SqliteDatabase(self.export.db_path)
        db.connect()

        for sql in self.export.sql_before:
            db.execute_sql(sql)

        if self.export.sql is None:
            query = "SELECT {columns}\nFROM {table}{where}{order_by}".format(
                columns=",".join(self.export.columns),
                table=self.export.table,
                where=self.export.where,
                order_by=self.export.order_by,
            )
        else:
            query = self.export.sql
        cursor = db.execute_sql(query)

        for chunk in chunker(
            cursor.fetchall(),
            size=self.export.chunk_size,
        ):
            yield ExportContext(
                export_kwargs={},
                columns=self.export.columns,
                data=chunk,
                data_orient=DataOrient.values,
            )

        for sql in self.export.sql_after:
            db.execute_sql(sql)
