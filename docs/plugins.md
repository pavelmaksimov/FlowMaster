# Plugins

## Создание скрипта экспорта данных для ETL оператора

```python
from flowmaster.operators.etl import ProviderAbstract, ExportAbstract, ExportContext, DataOrient
from pydantic import BaseModel


class MyProviderPolicy(BaseModel):
    # Через аннотацию типов задается набор параметров в политике export.
    # Все что здесь будет перечислено, можно будет задать в политике export.
    # https://pydantic-docs.helpmanual.io/
    my_str_param: str
    my_list_param: list[int]


class MyProviderExport(ExportAbstract):
    
    def __init__(self, notebook, *args, **kwargs):
        self.my_str_param = notebook.export.my_str_param
        self.my_list_param = notebook.export.my_list_param
        super(MyProviderExport, self).__init__(notebook, *args, **kwargs)

    def __call__(self, start_period, end_period):
        columns = ["col1", "col2"]
        for iter_export in range(10):
            data = [("value1", "value2")]
            yield ExportContext(data=data, columns=columns, data_orient=DataOrient.values)


class MyProvider(ProviderAbstract):
    name = "my_provider_name"
    policy_model = MyProviderPolicy
    export_class = MyProviderExport
```


## С аннотацией типов

```python
from typing import TYPE_CHECKING
from flowmaster.operators.etl import ProviderAbstract, ExportAbstract, ExportContext, DataOrient
from pydantic import BaseModel

if TYPE_CHECKING:
    from flowmaster.operators.etl.policy import ETLNotebook
    from datetime import datetime


class MyProviderPolicy(BaseModel):
    my_str_param: str
    my_list_param: list[int]


class MyProviderExport(ExportAbstract):

    def __init__(self, notebook: "ETLNotebook", *args, **kwargs):
        export: MyProviderPolicy = notebook.export
        self.my_str_param = export.my_str_param
        self.my_list_param = export.my_list_param
        super(MyProviderExport, self).__init__(notebook, *args, **kwargs)

    def __call__(self, start_period: "datetime", end_period: "datetime"):
        columns = ["col1", "col2"]
        for iter_export in range(10):
            data = [("value1", "value2")]
            yield ExportContext(data=data, columns=columns, data_orient=DataOrient.values)


class MyProvider(ProviderAbstract):
    name = "my_provider_name"
    policy_model = MyProviderPolicy
    export_class = MyProviderExport
```