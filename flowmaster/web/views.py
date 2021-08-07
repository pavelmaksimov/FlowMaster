import pathlib
from os.path import abspath

from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from flowmaster.enums import FlowStatus
from flowmaster.models import FlowItem
from flowmaster.service import (
    get_notebook,
    validate_notebook_policy,
    parse_and_validate_notebook_yaml,
    save_notebook,
    save_new_notebook,
    get_filepath_notebook,
    read_notebook_file,
    archive_notebook,
    unarchive_notebook,
    is_archive_notebook,
    iter_active_notebook_filenames,
    iter_archive_notebook_filenames,
    delete_notebook,
)
from flowmaster.setttings import Settings

webapp = FastAPI()
templates = Jinja2Templates(
    directory=str(pathlib.Path(abspath(__file__)).parent / "templates")
)


@webapp.get("/")
async def notebooks_view(request: Request):
    # TODO: Add pagination
    count_statuses_map = {
        (item.name, item.status): item.count
        for item in FlowItem.count_items_by_name_and_status()
    }
    count_names_map = {item.name: item.count for item in FlowItem.count_items_by_name()}
    notebooks = []

    for name in iter_active_notebook_filenames():
        data = {"name": name, "is_archive": False}
        validate, *args = get_notebook(name)

        data["count"] = count_names_map.get(name, 0)
        data["count_errors"] = sum(
            count_statuses_map.get((name, status), 0)
            for status in FlowStatus.error_statuses
        )
        data["count_fatal_errors"] = count_statuses_map.get(
            (name, FlowStatus.fatal_error), 0
        )
        data["validate"] = validate
        notebooks.append(data)

    for name in iter_archive_notebook_filenames():
        data = {"name": name, "is_archive": True, "validate": True}
        notebooks.append(data)

    return templates.TemplateResponse(
        "/pages/notebooks.html", context={"request": request, "notebooks": notebooks}
    )


@webapp.get("/notebook/create")
async def create_notebook_get_view(request: Request):
    return templates.TemplateResponse(
        "/pages/create.html",
        context={
            "request": request,
            "rows": 50,
        },
    )


@webapp.post("/notebook/create")
async def create_notebook_post_view(
    request: Request, name: str = Form(...), text: str = Form(...)
):
    text = text.replace("\n\n", "\n")
    name_error = ""

    validate_text, notebook_dict, text_error = parse_and_validate_notebook_yaml(text)
    if validate_text:
        filename = f"{name}.etl.yaml"  # TODO
        validate_policy, notebook_dict, policy, text_error = validate_notebook_policy(
            filename, notebook_dict
        )
        if validate_policy:
            result = save_new_notebook(filename, text, is_archive=True)

            if result is False:
                name_error = "A file with this name already exists"
            else:
                return RedirectResponse(f"/notebook/{filename}/detailed")

    return templates.TemplateResponse(
        "/pages/create.html",
        context={
            "request": request,
            "name": name.replace(".etl.yaml", ""),  # TODO
            "text": text,
            "rows": text.count("\n") + 3,
            "name_error": name_error,
            "text_error": text_error,
        },
    )


@webapp.get("/notebook/{name}/detailed")
@webapp.post("/notebook/{name}/detailed")
async def notebook_detailed_view(name: str, request: Request):
    validate, text, notebook_dict, policy, error = get_notebook(name)
    text = text.replace("\n\n", "\n")

    return templates.TemplateResponse(
        "/pages/detailed.html",
        context={
            "request": request,
            "name": name,
            "text": text,
            "error": error,
            "filepath": Settings.NOTEBOOKS_DIR / name,
        },
    )


@webapp.get("/notebook/{name}/edit")
async def edit_notebook_get_view(name: str, request: Request):
    text = read_notebook_file(name)
    validate, _, error = parse_and_validate_notebook_yaml(text)

    return templates.TemplateResponse(
        "/pages/edit.html",
        context={
            "request": request,
            "name": name,
            "text": text,
            "rows": text.count("\n") + 3,
            "error": error,
            "filepath": get_filepath_notebook(name),
        },
    )


@webapp.post("/notebook/{name}/edit")
async def edit_notebook_post_view(request: Request, name: str, text: str = Form(...)):
    text = text.replace("\n\n", "\n")
    validate, _, error = parse_and_validate_notebook_yaml(text)
    if validate:
        is_archive = is_archive_notebook(name)
        save_notebook(name, text, is_archive=is_archive)

        return RedirectResponse(f"/notebook/{name}/detailed")

    return templates.TemplateResponse(
        "/pages/edit.html",
        context={
            "request": request,
            "name": name,
            "text": text,
            "rows": text.count("\n") + 3,
            "error": error,
            "filepath": get_filepath_notebook(name),
        },
    )


@webapp.get("/notebook/{name}/delete")
@webapp.post("/notebook/{name}/delete")
def delete_notebook_view(name: str):
    delete_notebook(name)
    return RedirectResponse("/")


@webapp.get("/notebook/{name}/archive")
async def restart_task_view(name: str):
    archive_notebook(name)
    return RedirectResponse("/")


@webapp.get("/notebook/{name}/unarchive")
async def restart_task_view(name: str):
    unarchive_notebook(name)
    return RedirectResponse("/")


# Task


@webapp.get("/notebook/{name}/tasks")
async def tasks_view(name: str, request: Request):
    # TODO: Add pagination
    return templates.TemplateResponse(
        "/pages/tasks.html",
        context={
            "request": request,
            "tasks": FlowItem.iter_items(name, limit=1000, offset=0),
        },
    )


@webapp.get("/notebook/{name}/error-tasks")
async def error_tasks_view(name: str, request: Request):
    # TODO: Add pagination
    return templates.TemplateResponse(
        "/pages/tasks.html",
        context={
            "request": request,
            "tasks": FlowItem.iter_items(
                name, statuses=FlowStatus.error_statuses, limit=1000, offset=0
            ),
        },
    )


@webapp.get("/notebook/{name}/fatal-error-tasks")
async def fatal_error_tasks_view(name: str, request: Request):
    # TODO: Add pagination
    return templates.TemplateResponse(
        "/pages/tasks.html",
        context={
            "request": request,
            "tasks": FlowItem.iter_items(
                name, statuses=[FlowStatus.fatal_error], limit=1000, offset=0
            ),
        },
    )


@webapp.get("/notebook/{name}/task/{worktime_for_url}/logs")
async def log_view(name: str, worktime_for_url: str, request: Request):
    import re

    item: FlowItem = FlowItem.get_or_none(
        **{
            FlowItem.name.name: name,
            FlowItem.worktime.name: FlowItem.worktime_from_url(worktime_for_url),
        }
    )
    if item.logpath:
        if pathlib.Path.exists(pathlib.Path(item.logpath)):
            with open(item.logpath, "r", encoding="UTF8") as f:
                logtext = f.read()
                logtext = re.sub(r"\[\d\dm|\[\dm", "", logtext)
        else:
            logtext = "Logs not found: 'Logs file missing'"
    else:
        logtext = "Logs not found: 'Logs path missing'"

    return templates.TemplateResponse(
        "/pages/log.html",
        context={
            "request": request,
            "content": logtext,
            "filepath": item.logpath,
        },
    )


@webapp.get("/notebook/{name}/task/{worktime_for_url}/restart")
async def restart_task_view(name: str, worktime_for_url: str):
    worktime = FlowItem.worktime_from_url(worktime_for_url)
    FlowItem.recreate_item(name, worktime)

    return RedirectResponse(f"/notebook/{name}/tasks")
