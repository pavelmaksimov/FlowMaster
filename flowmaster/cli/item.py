import datetime as dt

import typer

from flowmaster.enums import Statuses
from flowmaster.models import FlowItem

app = typer.Typer()


@app.command("list")
def list_items(name: str, limit: int = 20):
    for i in FlowItem.iter_items(name, limit=limit):
        msg_parts = [
            f'  {i.worktime.strftime("%Y-%m-%dT%T").replace("T00:00:00", "")}  ',
            f"{i.status}  ",
            f"retries={i.retries}  ",
            f"duration={i.duration}  ",
            typer.style(f"log={i.log}", fg=typer.colors.WHITE) if i.log else "",
        ]

        if i.status in Statuses.error_statuses:
            msg_parts[1] = typer.style(msg_parts[1], fg=typer.colors.RED, bold=True)
        elif i.status == Statuses.add:
            msg_parts[1] = typer.style(msg_parts[1], fg=typer.colors.WHITE, bold=True)
        elif i.status == Statuses.run:
            msg_parts[1] = typer.style(msg_parts[1], fg=typer.colors.YELLOW, bold=True)
        elif i.status == Statuses.success:
            msg_parts[1] = typer.style(msg_parts[1], fg=typer.colors.GREEN, bold=True)

        typer.echo("".join(msg_parts))


@app.command()
def list_errors(name: str, limit: int = 1000):
    for i in FlowItem.iter_items(name, limit=limit):
        if i.status in Statuses.error_statuses:
            msg_parts = [
                f'  {i.worktime.strftime("%Y-%m-%dT%T").replace("T00:00:00", "")}  ',
                typer.style(f"{i.status}  ", fg=typer.colors.RED, bold=True),
                f"retries={i.retries}  ",
                f"duration={i.duration}  ",
                typer.style(f"log={i.log}", fg=typer.colors.WHITE) if i.log else "",
            ]

            typer.echo("".join(msg_parts))


@app.command()
def restart(
    name: str,
    from_time: dt.datetime = typer.Option(..., "--from_time", "-s"),
    to_time: dt.datetime = typer.Option(..., "--to_time", "-e"),
):
    for name_ in name.split(","):
        count = len(FlowItem.recreate_items(name, from_time=from_time, to_time=to_time))
        typer.secho(f"  {name_} {typer.style(f'{count=}', fg=typer.colors.WHITE)} OK")


@app.command()
def restart_errors(name: str):
    for name_ in name.split(","):
        count = len(
            FlowItem.recreate_items(name, filter_statuses=Statuses.error_statuses)
        )
        typer.secho(f"  {name_} {typer.style(f'{count=}', fg=typer.colors.WHITE)} OK")


@app.command()
def clear(name: str):
    for name_ in name.split(","):
        count = FlowItem.clear(name_)
        typer.secho(f"  {name_} {typer.style(f'{count=}', fg=typer.colors.WHITE)} OK")
