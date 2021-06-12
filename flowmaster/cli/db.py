import typer

app = typer.Typer()


@app.command()
def reset():
    from flowmaster.models import FlowItem

    FlowItem.drop_table()
    FlowItem.create_table()
