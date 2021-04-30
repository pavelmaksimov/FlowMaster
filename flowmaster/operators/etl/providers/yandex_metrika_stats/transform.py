from flowmaster.operators.etl.transform.service import Transform


class YandexMetrikaStatsTransform(Transform):
    null_values = ("", "--")
