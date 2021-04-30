from flowmaster.operators.etl.transform.service import Transform


class YandexMetrikaLogsTransform(Transform):
    null_values = ("", "--")
