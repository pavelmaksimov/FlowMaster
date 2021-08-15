from typing import TYPE_CHECKING, Iterator

from flowmaster.exceptions import AuthError
from flowmaster.operators.etl.dataschema import ExportContext
from flowmaster.operators.etl.enums import DataOrient
from flowmaster.operators.etl.providers.abstract import ExportAbstract

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebookPolicy
    from flowmaster.operators.etl.providers.google_sheets.policy import (
        GoogleSheetsExportPolicy,
    )


class GoogleSheetsExport(ExportAbstract):
    def __init__(self, notebook: "ETLNotebookPolicy", *args, **kwargs):
        super(GoogleSheetsExport, self).__init__(notebook, *args, **kwargs)

    def drop_extra_columns(self, data):
        if self.export.with_columns:
            column_names = []
            data_ = []
            for i in range(len(data)):
                column_values = data[i]
                if column_values:
                    colname = column_values.pop(0)
                    if colname in self.export.columns:
                        column_names.append(colname)
                        data_.append(column_values)
            data = data_
        else:
            column_names = self.export.columns
            data = data[: len(self.export.columns)]

        return column_names, data

    def fill_empty_cells(self, data):
        if data:
            max_col_length = max(len(col) for col in data)
            for col in data:
                while len(col) < max_col_length:
                    col += [None] * (max_col_length - len(col))

        return data

    def __call__(self, *args, **kwargs) -> Iterator[ExportContext]:
        import pygsheets
        from google.auth.exceptions import (
            RefreshError,
            OAuthError,
            GoogleAuthError,
            UserAccessTokenError,
        )
        from googleapiclient import errors
        from pygsheets import Worksheet

        self.logger.info("Exportation data")

        self.export: "GoogleSheetsExportPolicy" = self.model_templating(
            *args, model=self.notebook.export
        )
        api = pygsheets.authorize(**self.export.credentials.dict())
        try:
            sheet = api.open_by_key(self.export.sheet_id)
            page: Worksheet = sheet.worksheet(property="id", value=self.export.page_id)
            data = page.get_values(
                start=self.export.start,
                end=self.export.end,
                returnas="matrix",
                majdim="COLUMNS",
                include_tailing_empty=False,
                include_tailing_empty_rows=False,
            )
        except (RefreshError, OAuthError, GoogleAuthError, UserAccessTokenError) as exc:
            raise AuthError(exc)

        except errors.HttpError as exc:
            if exc.resp.status == 403:
                raise PermissionError(exc)
            raise

        column_names, data = self.drop_extra_columns(data)
        data = self.fill_empty_cells(data)

        yield ExportContext(
            columns=column_names,
            data=data,
            data_orient=DataOrient.columns,
            export_kwargs={
                "sheet_id": self.export.sheet_id,
                "sheet_name": sheet.title,
                "sheet_url": sheet.url,
                "page_id": self.export.page_id,
                "page_name": page.title,
            },
        )

# TODO: add Annotation