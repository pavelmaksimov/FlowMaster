from flowmaster.operators.etl.transform.service import Transform


class YandexDirectTransform(Transform):
    null_values = ("", "--")
