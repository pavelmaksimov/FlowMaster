import typer

import flowmaster.setttings as setttings
from flowmaster.models import FlowItem, FlowStatus
from flowmaster.utils.yaml_helper import YamlHelper

app = typer.Typer()


@app.command("list")
def list_config():
    for file_name, config in YamlHelper.iter_parse_file_from_dir(
        setttings.FLOW_CONFIGS_DIR, match=".flow"
    ):
        typer.echo(f"  {file_name}")


@app.command()
def validate():
    from flowmaster.operators.etl.config import ETLFlowConfig

    for file_name, config in YamlHelper.iter_parse_file_from_dir(
        setttings.FLOW_CONFIGS_DIR, match=".flow"
    ):
        ETLFlowConfig(name=file_name, **config)
        typer.echo(f"  {file_name} OK")


@app.command()
def errors():
    for name in list_config():
        count = FlowItem.count_items(name, statuses=[FlowStatus.error_statuses])
        if count > 0:
            count_text = typer.style(count, fg=typer.colors.RED, bold=True)
        else:
            count_text = typer.style(count, fg=typer.colors.GREEN, bold=True)

        name = typer.style(name, fg=typer.colors.WHITE, bold=True)
        typer.echo(f"  {name} {count_text}")
