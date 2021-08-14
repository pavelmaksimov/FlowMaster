from typing import TYPE_CHECKING, Iterator

import peewee

from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.providers.abstract import ExportAbstract
from flowmaster.utils import chunker

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook
    from flowmaster.operators.etl.providers.mysql.policy import MySQLExportPolicy


class MySQLExport(ExportAbstract):
    def __init__(self, notebook: "ETLNotebook", *args, **kwargs):
        super(MySQLExport, self).__init__(notebook, *args, **kwargs)

    def __call__(self, *args, **kwargs) -> Iterator[ExportContext]:
        self.logger.info("Exportation data")

        self.export: "MySQLExportPolicy" = self.model_templating(
            *args, model=self.notebook.export
        )

        db = peewee.MySQLDatabase(
            database=self.export.database,
            user=self.export.user,
            password=self.export.password,
            host=self.export.host,
            port=self.export.port,
        )
        db.connect()

        for sql in self.export.sql_before:
            db.execute_sql(sql)

        if self.export.sql is None:
            query = "SELECT {columns}\nFROM {table}{where}{order_by}".format(
                columns=f'`{"`,`".join(self.export.columns)}`',
                table=self.export.table,
                where=self.export.where,
                order_by=self.export.order_by,
            )
        else:
            query = self.export.sql
        cursor = db.execute_sql(query)
        d = list(cursor.fetchall())
        for chunk in chunker(
            d,
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
