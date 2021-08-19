from flowmaster.operators.etl.policy import ETLNotebook
from flowmaster.utils import KlassCollection, KlassT


class NotebooksCollection(KlassCollection):
    name_attr_of_klass = "operator"

    def set(self, klass: KlassT, /) -> None:
        self.__dict__["_name_attr_of_klasses"][klass.Config.operator] = klass
        return super().set(klass)


Notebooks = NotebooksCollection(ETLNotebook)
