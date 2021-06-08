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

    typer.echo(f"\nAPP_HOME={APP_HOME}")

    pathlib.Path.mkdir(APP_HOME, exist_ok=True)
    pathlib.Path.mkdir(FILE_STORAGE_DIR, exist_ok=True)
    pathlib.Path.mkdir(LOGS_DIR, exist_ok=True)
    pathlib.Path.mkdir(FLOW_CONFIGS_DIR, exist_ok=True)
    pathlib.Path.mkdir(PLUGINS_DIR, exist_ok=True)

    if not pathlib.Path.exists(POOL_CONFIG_FILEPATH):
        with open(POOL_CONFIG_FILEPATH, "w") as f:
            f.write("flows: 100\n")

    database.create_tables([FlowItem])


def prepare_for_run(dry_run: bool = False):
    init()

    typer.echo("\n===================" "\nFlowMaster" "\n===================\n")

    from flowmaster.models import FlowItem

    # Clearing statuses for unfulfilled flows.
    FlowItem.clear_statuses_of_lost_items()

    if dry_run:
        FlowItem.delete().where("fakedata.etl.flow" in FlowItem.name).execute()


@app.command()
def run(interval: int = 20, dry_run: bool = False):
    prepare_for_run(dry_run=dry_run)

    from flowmaster.utils.local_executor import sync_executor

    typer.echo("Executor: sync_executor\n")

    sync_executor(interval=interval, dry_run=dry_run)


@app.command()
def run_thread(workers: int = 2, interval: int = 20, dry_run: bool = False):
    prepare_for_run(dry_run=dry_run)

    from flowmaster.operators.base.work import ordering_flow_tasks
    from flowmaster.executors import ThreadAsyncExecutor

    typer.echo("Executor: ThreadAsyncExecutor\n")

    order_task_func = partial(ordering_flow_tasks, dry_run=dry_run)
    executor = ThreadAsyncExecutor(ordering_task_func=order_task_func)
    executor.start(interval=interval, workers=workers)


if __name__ == "__main__":
    app()
