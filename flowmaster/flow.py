from flowmaster.notebook import Notebooks
from flowmaster.operators.etl.core import ETLOperator
from flowmaster.utils import KlassCollection


class FlowCollection(KlassCollection):
    name_attr_of_klass = "name"
    name_attr_in_kwargs = "operator"

    def __call__(self, notebook):
        if isinstance(notebook, dict):
            notebook = Notebooks(**notebook)
        return super(FlowCollection, self).__call__(notebook)


Flow = FlowCollection(ETLOperator)
