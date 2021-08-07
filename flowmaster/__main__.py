import platform
from functools import partial

import typer
import uvicorn

import flowmaster.cli.db
import flowmaster.cli.item
import flowmaster.cli.notebook
from flowmaster import prepare
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
        FlowItem.delete().where("fakedata.etl.flow" in FlowItem.name).execute()


def run_web(port):
    kwargs = dict(app=webapp, host="0.0.0.0", port=int(port))
    if platform.system() == "Windows":
        import threading

        web_thread = threading.Thread(
            target=uvicorn.run,
            name="Flowmaster_web",
            daemon=True,
            kwargs=kwargs,
        )
    else:
        import multiprocessing

        web_thread = multiprocessing.Process(
            target=uvicorn.run,
            name="Flowmaster_web",
            daemon=True,
            kwargs=kwargs,
        )
    web_thread.start()
    typer.echo(f"WEB UI: 0.0.0.0:{port}\n")


@app.command()
def run_local(interval: int = 20, dry_run: bool = False):
    prepare_items(dry_run=dry_run)

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

    prepare()
    prepare_items(dry_run=dry_run)
    run_web(port)

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
