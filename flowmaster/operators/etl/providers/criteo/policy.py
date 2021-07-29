from typing import Optional, Literal

from pydantic import BaseModel, PositiveInt

from flowmaster.operators.base.policy import BasePolicy


class CriteoExportPolicy(BasePolicy):
    class CredentialsPolicy(BaseModel):
        # https://developers.criteo.com/marketing-solutions/docs/onboarding-checklist
        # From API key file.
        client_id: str
        client_secret: str

    class StatsV202104ParamsPolicy(BaseModel):
        """
        https://developers.criteo.com/marketing-solutions/docs/analytics
        https://github.com/criteo/criteo-python-marketing-transition-sdk/blob/main/docs/StatisticsReportQueryMessage.md
        """

        # https://developers.criteo.com/marketing-solutions/docs/dimensions
        dimensions: list[str]
        # https://developers.criteo.com/marketing-solutions/docs/metrics
        metrics: list[str]
        # https://developers.criteo.com/marketing-solutions/docs/currencies-supported
        currency: str
        # If you do not provide any advertiserIds value,
        # statistics for all advertisers in your portfolio will be returned.
        advertiser_ids: Optional[list[str]] = None
        # https://developers.criteo.com/marketing-solutions/docs/timezones-supported
        timezone: str = "UTC"

    api_version: Literal["202104"]
    credentials: CredentialsPolicy
    resource: Literal["stats"]
    params: StatsV202104ParamsPolicy
    chunk_size: Optional[PositiveInt]
    concurrency: PositiveInt = 5
