import datetime as dt

import pendulum
import typer

from flowmaster.models import FlowItem, FlowStatus
from flowmaster.setttings import FLOW_CONFIGS_DIR
from flowmaster.utils.yaml_helper import YamlHelper

app = typer.Typer()


@app.command("list")
def list_items(name: str, limit: int = 20):
    for i in FlowItem.iter_items(name, limit):
        msg_parts = [
            f'  {i.worktime.strftime("%Y-%m-%dT%T").replace("T00:00:00", "")}  ',
            f"{i.status}  ",
            f"retries={i.retries}  ",
            f"duration={i.duration}  ",
            typer.style(f"log={i.log}", fg=typer.colors.WHITE) if i.log else "",
        ]

        if i.status in FlowStatus.error_statuses:
            msg_parts[1] = typer.style(msg_parts[1], fg=typer.colors.RED, bold=True)
        elif i.status == FlowStatus.add:
            msg_parts[1] = typer.style(msg_parts[1], fg=typer.colors.WHITE, bold=True)
        elif i.status == FlowStatus.run:
            msg_parts[1] = typer.style(msg_parts[1], fg=typer.colors.YELLOW, bold=True)
        elif i.status == FlowStatus.success:
            msg_parts[1] = typer.style(msg_parts[1], fg=typer.colors.GREEN, bold=True)

        typer.echo("".join(msg_parts))


@app.command()
def list_errors(name: str, limit: int = 1000):
    for i in FlowItem.iter_items(name, limit):
        if i.status in FlowStatus.error_statuses:
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
    start_time: dt.datetime = typer.Option(..., "--start_time", "-s"),
    end_time: dt.datetime = typer.Option(..., "--end_time", "-e"),
):
    for name_ in name.split(","):
        if start_time or end_time:
            # Apply timezone.
            for file_name, config in YamlHelper.iter_parse_file_from_dir(
                FLOW_CONFIGS_DIR, match=name_
            ):
                tz = config["work"]["schedule"]["timezone"]

                if start_time:
                    start_time = start_time.replace(tzinfo=pendulum.timezone(tz))

                if end_time:
                    end_time = end_time.replace(tzinfo=pendulum.timezone(tz))

                break

        count = FlowItem.change_status(
            name_, new_status=FlowStatus.add, from_time=start_time, to_time=end_time
        )
        typer.secho(f"  {name_} {typer.style(f'{count=}', fg=typer.colors.WHITE)} OK")


@app.command()
def restart_errors(name: str):
    for name_ in name.split(","):
        count = FlowItem.change_status(
            name_, new_status=FlowStatus.add, filter_statuses=FlowStatus.error_statuses
        )
        typer.secho(f"  {name_} {typer.style(f'{count=}', fg=typer.colors.WHITE)} OK")


@app.command()
def clear(name: str):
    for name_ in name.split(","):
        count = FlowItem.clear(name_)
        typer.secho(f"  {name_} {typer.style(f'{count=}', fg=typer.colors.WHITE)} OK")
