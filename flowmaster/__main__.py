import pathlib
from functools import partial

import typer

import flowmaster.cli.config
import flowmaster.cli.db
import flowmaster.cli.item
from flowmaster.setttings import (
    APP_HOME,
    FLOW_CONFIGS_DIR,
    POOL_CONFIG_FILEPATH,
    FILE_STORAGE_DIR,
    LOGS_DIR,
    PLUGINS_DIR,
)

app = typer.Typer()
app.add_typer(flowmaster.cli.config.app, name="config")
app.add_typer(flowmaster.cli.db.app, name="db")
app.add_typer(flowmaster.cli.item.app, name="item")


@app.command()
def init():
    from flowmaster.models import database, FlowItem

    typer.echo(f"APP_HOME={APP_HOME}")

    pathlib.Path.mkdir(APP_HOME, exist_ok=True)
    pathlib.Path.mkdir(FILE_STORAGE_DIR, exist_ok=True)
    pathlib.Path.mkdir(LOGS_DIR, exist_ok=True)
    pathlib.Path.mkdir(FLOW_CONFIGS_DIR, exist_ok=True)
    pathlib.Path.mkdir(PLUGINS_DIR, exist_ok=True)

    if not pathlib.Path.exists(POOL_CONFIG_FILEPATH):
        with open(POOL_CONFIG_FILEPATH, "w") as f:
            f.write("flows: 100\n")

    database.create_tables([FlowItem])


@app.command()
def run(workers: int = 1, interval: int = 20, dry_run: bool = False):
    init()

    typer.echo("\n===================" "\nRun FlowMaster" "\n===================\n")

    from flowmaster.models import FlowItem
    from flowmaster.operators.base.work import order_flow
    from flowmaster.utils.executor import Executor

    # Clearing statuses for unfulfilled flows.
    FlowItem.clear_statuses_of_lost_items()

    if dry_run:
        FlowItem.delete().where(FlowItem.name == "fakedata.etl.flow.yml").execute()

    order_task_func = partial(order_flow, dry_run=dry_run)
    executor = Executor(order_task_func=order_task_func)
    executor.start(order_interval=interval, workers=workers)


if __name__ == "__main__":
    app()
