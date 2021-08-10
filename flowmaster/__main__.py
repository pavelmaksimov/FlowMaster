import threading
from functools import partial

import typer
import uvicorn

import flowmaster.cli.db
import flowmaster.cli.item
import flowmaster.cli.notebook
from flowmaster import create_initial_dirs_and_files
from flowmaster.setttings import Settings
from flowmaster.web.views import webapp

app = typer.Typer()
app.add_typer(flowmaster.cli.notebook.app, name="notebook")
app.add_typer(flowmaster.cli.db.app, name="db")
app.add_typer(flowmaster.cli.item.app, name="item")


def prepare_items(dry_run: bool = False):
    typer.echo("\n===================" "\nFlowMaster" "\n===================\n")

    from flowmaster.models import FlowItem

    # Clearing statuses for unfulfilled flows.
    FlowItem.clear_statuses_of_lost_items()

    if dry_run:
        typer.echo(f"Dry-run mode!")
        FlowItem.clear("fakedata.etl.flow")


def run_web(port):
    kwargs = dict(app=webapp, host="0.0.0.0", port=int(port), loop="asyncio")
    web_thread = threading.Thread(
        target=uvicorn.run,
        name="Flowmaster_web",
        daemon=True,
        kwargs=kwargs,
    )
    web_thread.start()
    typer.echo(f"WEB UI: 0.0.0.0:{port}\n")


@app.command()
def run_local(
    interval: int = 20,
    dry_run: bool = False,
    port: int = Settings.WEBUI_PORT,
):
    prepare_items(dry_run=dry_run)
    run_web(port)

    from flowmaster.utils.local_executor import sync_executor

    typer.echo("Executor: sync_executor\n")

    sync_executor(interval=interval, dry_run=dry_run)


@app.command()
def run(
    workers: int = 2,
    interval: int = 20,
    dry_run: bool = False,
    port: int = Settings.WEBUI_PORT,
):
    typer.echo(f"\nAPP_HOME={Settings.APP_HOME}")

    create_initial_dirs_and_files()
    prepare_items(dry_run=dry_run)
    run_web(port)

    from flowmaster.operators.base.work import ordering_flow_tasks
    from flowmaster.executors import ThreadAsyncExecutor

    typer.echo("Executor: ThreadAsyncExecutor")
    typer.echo(f"Number of workers: {workers}")
    typer.echo(f"Scheduler interval: {interval}")

    order_task_func = partial(ordering_flow_tasks, dry_run=dry_run)
    executor = ThreadAsyncExecutor(ordering_task_func=order_task_func)
    executor.start(interval=interval, workers=workers)


if __name__ == "__main__":
    app()
