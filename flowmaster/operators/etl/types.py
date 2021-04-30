from typing import Literal


class DataOrient:
    values = "values"  # list[list | tuple]
    columns = "columns"  # list[list]
    dict = "dict"  # list[dict]

    LiteralT = Literal["values", "columns", "dict"]
