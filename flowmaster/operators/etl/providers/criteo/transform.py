from flowmaster.operators.etl.transform.service import Transform


class CriteoTransform(Transform):
    null_values = ("", "--")
