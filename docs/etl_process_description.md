# Как устроен ETL в FlowMaster.

Размерем на упрощенном примере.

Логика экспорта данных реализуется в классе `Export`.
Магическом метод `__call__` класса возвращает данные.
Как правило источники данные, отдают данные по частям, 
поэтому метод должен итерироваться `Iterator`.
```python
class Export:
    def __call__(self) -> Iterator:
        yield [("1", "foo"), ] 
        yield [("2", "bar"), ] 
```
Этот участок кода уже может и должен работать сам по себе.

Реализацию классов экспорта данных [здесь](../flowmaster/operators/etl/providers).


Логика преобразования данных реализуется в классе `Transform`
Магическом метод `__call__` класса получает экспортированные данные, 
а возвращает преобразованные данные.
Имеется встроенный преобразователь данных для каждого источника данных, 
поэтому вам не нужно об этом заботиться.
```python
class Transform:
    def __call__(self, data) -> Iterable:
        # Data transformation.
        ...
        return data
```


Логика загрузки данных реализуется в классе `Load`.
Магическом метод `__call__` класса получает преобразованные данные 
и вставляет их в хранилище.
Подготовка хранилища к загрузке данных осуществляется в магическом методе 
контекстного менеджера `__enter__`.
А в магическом методе контекстного менеджера `__exit__` осуществляется логика 
после завершения загрузки данных в хранилище.
```python
class Load:
    def __call__(self, data) -> None:
        # Inserting data into storage.
        ...

    def __enter__(self):
        # Preparing the storage for loading data.
        # For example, create a staging table in the database.
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Executed after all pieces of data have been loaded.
        # For example, you can delete the staging table here.
        ...
```
Пример реализации скрипта загрузки можно найти [здесь](../flowmaster/operators/etl/loaders)


Все эти классы объединяются в одном классе ```ETLOperator```
```python
class ETLOperator:
    def __init__(self):
        self.Export = Export()
        self.Transform = Transform()
        self.Load = Load()

    def __call__(self, *args, **kwargs):
        with self.Load as load:
            for iter_export_data in self.Export():
                data = self.Transform(iter_export_data)
                load(data)

flow = ETLOperator()
flow()
```
Подробнее, как устроен оператор, вы можете изучить [здесь](../flowmaster/operators/etl/core.py).

[Как написать свой скрипт экспорта данных](plugins.md)
