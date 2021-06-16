import typer

from flowmaster.models import FlowItem, FlowStatus
from flowmaster.setttings import Settings
from flowmaster.utils.yaml_helper import YamlHelper

app = typer.Typer()


@app.command("list")
def list_notebook():
    for file_name, _ in YamlHelper.iter_parse_file_from_dir(
        Settings.NOTEBOOKS_DIR, match=".flow"
    ):
        typer.echo(f"  {file_name}")


@app.command()
def validate():
    from flowmaster.operators.etl.policy import ETLNotebook

    for file_name, notebook_dict in YamlHelper.iter_parse_file_from_dir(
        Settings.NOTEBOOKS_DIR, match=".flow"
    ):
        ETLNotebook(name=file_name, **notebook_dict)
        typer.echo(f"  {file_name} OK")


@app.command()
def errors():
    for name in list_notebook():
        count = FlowItem.count_items(name, statuses=[FlowStatus.error_statuses])
        if count > 0:
            count_text = typer.style(count, fg=typer.colors.RED, bold=True)
        else:
            count_text = typer.style(count, fg=typer.colors.GREEN, bold=True)

        name = typer.style(name, fg=typer.colors.WHITE, bold=True)
        typer.echo(f"  {name} {count_text}")
