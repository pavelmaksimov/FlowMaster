import pathlib
from functools import partial
from multiprocessing import Process

import typer
import uvicorn

import flowmaster.cli.db
import flowmaster.cli.item
import flowmaster.cli.notebook
from flowmaster.setttings import Settings
from flowmaster.web.views import webapp

app = typer.Typer()
app.add_typer(flowmaster.cli.notebook.app, name="notebook")
app.add_typer(flowmaster.cli.db.app, name="db")
app.add_typer(flowmaster.cli.item.app, name="item")


@app.command()
def init():
    from flowmaster.models import database, FlowItem

    typer.echo(f"\nAPP_HOME={Settings.APP_HOME}")

    pathlib.Path.mkdir(Settings.APP_HOME, exist_ok=True)
    pathlib.Path.mkdir(Settings.FILE_STORAGE_DIR, exist_ok=True)
    pathlib.Path.mkdir(Settings.LOGS_DIR, exist_ok=True)
    pathlib.Path.mkdir(Settings.NOTEBOOKS_DIR, exist_ok=True)
    pathlib.Path.mkdir(Settings.ARCHIVE_NOTEBOOKS_DIR, exist_ok=True)
    pathlib.Path.mkdir(Settings.PLUGINS_DIR, exist_ok=True)

    if not pathlib.Path.exists(Settings.POOL_CONFIG_FILEPATH):
        with open(Settings.POOL_CONFIG_FILEPATH, "w") as f:
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
def run_local(interval: int = 20, dry_run: bool = False):
    prepare_for_run(dry_run=dry_run)

    from flowmaster.utils.local_executor import sync_executor

    typer.echo("Executor: sync_executor\n")

    sync_executor(interval=interval, dry_run=dry_run)


@app.command()
def run(workers: int = 2, interval: int = 20, dry_run: bool = False, port=Settings.WEBUI_PORT):
    prepare_for_run(dry_run=dry_run)

    webserver_thread = Process(
        target=uvicorn.run,
        name="Flowmaster_web",
        kwargs=dict(app=webapp, host="0.0.0.0", port=port),
    )
    webserver_thread.start()
    typer.echo(f"WEB UI: 0.0.0.0:{port}\n")

    from flowmaster.operators.base.work import ordering_flow_tasks
    from flowmaster.executors import ThreadAsyncExecutor

    if dry_run:
        typer.echo(f"Dry-run mode!")
    typer.echo("Executor: ThreadAsyncExecutor")
    typer.echo(f"Number of workers: {workers}")
    typer.echo(f"Scheduler interval: {interval}")

    order_task_func = partial(ordering_flow_tasks, dry_run=dry_run)
    executor = ThreadAsyncExecutor(ordering_task_func=order_task_func)
    executor.start(interval=interval, workers=workers)


if __name__ == "__main__":
    app()
