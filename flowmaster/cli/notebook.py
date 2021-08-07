import typer

from flowmaster.enums import FlowStatus
from flowmaster.models import FlowItem
from flowmaster.service import (
    iter_active_notebook_filenames,
    iter_archive_notebook_filenames,
    get_notebook,
)

app = typer.Typer()


@app.command("list")
def list_notebook():
    for name, _ in iter_active_notebook_filenames():
        typer.echo(f"  {name}")

    for name, _ in iter_archive_notebook_filenames():
        typer.echo(f"  {name} (archive)")


@app.command()
def validate():
    for name, _ in iter_active_notebook_filenames():
        validate, text, notebook_dict, policy, error = get_notebook(name)
        if validate:
            typer.echo(f"  {name} - OK")

    for name, _ in iter_archive_notebook_filenames():
        validate, text, notebook_dict, policy, error = get_notebook(name)
        if validate:
            typer.echo(f"  {name} (archive) - OK")


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
