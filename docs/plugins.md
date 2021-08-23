# Plugins

## Creation of data export script for ETL operator

```python
from typing import TYPE_CHECKING, Iterator

from flowmaster.operators.base.policy import BasePolicy
from flowmaster.operators.etl.providers.abstract import ProviderAbstract, ExportAbstract
from flowmaster.operators.etl import ExportContext, DataOrient

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook
    from datetime import datetime


class MyProviderPolicy(BasePolicy):
    # Everything that will be listed here can be set in the 'export' policy.
    # 'Pydantic' validates attributes.
    my_columns: list[str]
    token: str
    rows: int


class MyProviderExport(ExportAbstract):

    def __init__(self, notebook: "ETLNotebook", *args, **kwargs):
        self.export: MyProviderPolicy = notebook.export
        super(MyProviderExport, self).__init__(notebook, *args, **kwargs)

    def __call__(self, start_period: "datetime", end_period: "datetime") -> Iterator[ExportContext]:
        self.logger.info("Exportation data")

        for iter_export in range(self.export.rows):
            data = [f"value_of_col_{name}" for name in self.export.my_columns]
            yield ExportContext(
                data=data,
                columns=self.export.my_columns,
                data_orient=DataOrient.values
            )


class MyProvider(ProviderAbstract):
    name = "my_provider_name"
    policy_model = MyProviderPolicy
    export_class = MyProviderExport
```


## Testing

### Create pytest

```python
def test_my_provider(
    csv_transform_policy, csv_load_policy
):
    """Imports from flowmaster must be placed inside the tested functions."""

    from flowmaster.operators.etl.providers import Providers
    from flowmaster.operators.etl.loaders import Loaders
    from flowmaster.notebook import Notebooks
    
    assert "my_provider_name" in Providers

    # Get class 'MyProvider'
    MyProvider = Providers["my_provider_name"]
    export_policy = MyProvider.policy_model(my_columns=["col1"], token="", rows=3)
    notebook = Notebooks.ETLNotebook(
        name="__test_my_provider__",
        provider=MyProvider.name,
        storage=Loaders.CSVLoader.name,
        export=export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )
    # Init class
    my_provider = MyProvider(notebook)

    for export_context in my_provider.Export(start_period=None, end_period=None):
        assert export_context.data == ["value_of_col_col1"]
        assert export_context.columns == ["col1"]
```


### Dry-run

```python
import datetime as dt
from typing import Union

from flowmaster.flow import Flow
from flowmaster.operators.etl.loaders import Loaders
from flowmaster.notebook import Notebooks
from flowmaster.operators.etl.providers import Providers
from flowmaster.setttings import Settings


def get_notebook() -> Union[dict, Notebooks.ETLNotebook]:
    MyProvider = Providers["my_provider_name"]
    export_policy = MyProvider.policy_model(my_columns=["col1"], token="", rows=3)
    csv_transform_policy = Loaders.CSVLoader.transform_policy_model(error_policy="default")
    csv_load_policy = Loaders.CSVLoader.policy_model(
        path=Settings.FILE_STORAGE_DIR,
        file_name=f"test_my_provider.csv",
        save_mode="w"
    )
    return Notebooks.ETLNotebook(
        name="__test_my_provider__",
        provider=MyProvider.name,
        storage=Loaders.CSVLoader.name,
        export=export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
    )


def main(start_period, end_period):
    notebook = get_notebook()
    flow = Flow(notebook)
    flow.dry_run(start_period=start_period, end_period=end_period)

    with flow.Load.open_file(mode="r") as loadfile:
        text = loadfile.read()

    assert text

    
if __name__ in "__main__":
    main(dt.datetime.now(), dt.datetime.now())
```