from flowmaster.operators.etl.providers.abstract import ProviderAbstract
from flowmaster.operators.etl.providers.google_sheets.export import GoogleSheetsExport
from flowmaster.operators.etl.providers.google_sheets.policy import (
    GoogleSheetsExportPolicy,
)


class GoogleSheetsProvider(ProviderAbstract):
    name = "google_sheets"
    policy_model = GoogleSheetsExportPolicy
    export_class = GoogleSheetsExport
