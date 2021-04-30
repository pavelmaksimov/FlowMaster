# Plugins

## Создание скрипта экспорта данных

```python
from typing import TYPE_CHECKING
from flowmaster.operators.etl import ProviderAbstract, ExportAbstract, ExportContext, DataOrient
from pydantic import BaseModel

if TYPE_CHECKING:
    from flowmaster.operators.etl.config import ETLFlowConfig
    from datetime import datetime


class MyProviderPolicy(BaseModel):
    my_str_param: str
    my_int_param: int


class MyProviderExport(ExportAbstract):
    data_orient = DataOrient.values
    
    def __init__(self, config: "ETLFlowConfig", *args, **kwargs):
        export: MyProviderPolicy = config.export
        self.my_str_param = export.my_str_param
        self.my_int_param = export.my_int_param
        super(MyProviderExport, self).__init__(config, *args, **kwargs)

    def __call__(self, start_period: "datetime", end_period: "datetime"):
        columns = ["col1", "col2"]
        for iter_export in range(10):
            data = [("value1", "value2")]
            yield ExportContext(data=data, columns=columns)


class MyProvider(ProviderAbstract):
    name = "my_provider_name"
    config_model = MyProviderPolicy
    export_class = MyProviderExport
```