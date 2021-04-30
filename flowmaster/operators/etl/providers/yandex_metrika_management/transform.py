from flowmaster.operators.etl.transform.service import Transform


class YandexMetrikaManagementTransform(Transform):
    null_values = ("", None)
