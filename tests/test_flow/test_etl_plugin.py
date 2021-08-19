def test_provider_etl_plugin_from_doc(create_etl_plugin_from_doc):
    from flowmaster.operators.etl.providers import Providers

    assert "my_provider_name" in Providers.name_attr_of_klasses()
    assert "MyProvider" in Providers.klass_names()


def test_etl_plugin_from_doc(
    create_etl_plugin_from_doc, work_policy, csv_transform_policy, csv_load_policy
):
    from flowmaster.operators.etl.providers import Providers
    from flowmaster.operators.etl.loaders import Storages
    from flowmaster.operators.etl.policy import ETLNotebook

    MyProvider = Providers["my_provider_name"]
    export_policy = MyProvider.policy_model(my_columns=["col1"], token="", rows=3)
    notebook = ETLNotebook(
        name="__test_my_provider__",
        provider=MyProvider.name,
        storage=Storages.CSVLoader.name,
        export=export_policy,
        transform=csv_transform_policy,
        load=csv_load_policy,
        work=work_policy,
    )
    my_provider = MyProvider(notebook)

    for export_context in my_provider.Export(start_period=None, end_period=None):
        assert export_context.data == ["value_of_col_col1"]
        assert export_context.columns == ["col1"]
