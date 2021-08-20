

def test_provider_etl_plugin_from_doc(create_etl_plugin_from_doc):
    from flowmaster.operators.etl.providers import Providers

    assert "my_provider_name" in Providers.name_attr_of_klasses()
    assert "MyProvider" in Providers.klass_names()


def test_etl_plugin_from_doc(
    create_etl_plugin_from_doc, work_policy, csv_transform_policy, csv_load_policy
):
    from flowmaster.operators.etl.providers import Providers
    from flowmaster.operators.etl.loaders import Loaders
    from flowmaster.notebook import Notebooks

    MyProvider = Providers["my_provider_name"]
    export_policy = MyProvider.policy_model(my_columns=["col1"], token="", rows=3)
    notebook = Notebooks.ETLNotebook(
        name="__test_my_provider__",
        provider=MyProvider.name,
        storage=Loaders.CSVLoader.name,
        export=export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
        work=work_policy,
    )
    my_provider = MyProvider(notebook)

    for export_context in my_provider.Export(start_period=None, end_period=None):
        assert export_context.data == ["value_of_col_col1"]
        assert export_context.columns == ["col1"]


def test_dry_run(create_etl_plugin_from_doc):
    import datetime as dt
    from typing import Union

    from flowmaster.flow import Flow
    from flowmaster.notebook import Notebooks
    from flowmaster.setttings import Settings

    def get_notebook() -> Union[dict, Notebooks.ETLNotebook]:
        MyProvider = Flow.ETLOperator.Providers["my_provider_name"]
        export_policy = MyProvider.policy_model(my_columns=["col1"], token="", rows=3)
        csv_transform_policy = Flow.ETLOperator.Loaders.CSVLoader.transform_policy_model(error_policy="default")
        csv_load_policy = Flow.ETLOperator.Loaders.CSVLoader.policy_model(
            path=Settings.FILE_STORAGE_DIR,
            file_name=f"test_my_provider.csv",
            save_mode="w"
        )
        return Notebooks.ETLNotebook(
            name="__test_my_provider__",
            provider=MyProvider.name,
            storage=Flow.ETLOperator.Loaders.CSVLoader.name,
            export=export_policy,
            transform=csv_transform_policy,
            load=csv_load_policy,
        )

    def test_flow():
        notebook = get_notebook()
        flow = Flow(notebook)
        flow.dry_run(dt.datetime.now(), dt.datetime.now())

        with flow.Load.open_file(mode="r") as loadfile:
            text = loadfile.read()

        assert text

    test_flow()
