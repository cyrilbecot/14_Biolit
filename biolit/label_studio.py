import os
import polars as pl
from dotenv import load_dotenv
import structlog

from datetime import datetime

from label_studio_sdk import LabelStudio
from label_studio_sdk.data_manager import Filters, Column, Operator, Type

LOGGER = structlog.get_logger()
load_dotenv()


def push_tasks_label_studio(project_title: str, df: pl.DataFrame):
    api_key = os.getenv("LABEL_STUDIO_API_KEY")
    url = "http://label-studio:8080"

    client = LabelStudio(base_url=url, api_key=api_key)
    projects = client.projects.list()

    project_id = None
    for project in projects:
        if project.title == project_title:
            project_id = project.id
            LOGGER.info(f"Projet ID={project.id}, Nom={project.title} exists")
            break

    if project_id is None:
        LOGGER.info(f"The project {project_title} does not exist.")
        return

    tasks = []

    for row in df.to_dicts():

        tasks.append({
            "data": {
                "image": {row["path_s3"]},
                "id_crops": row["id_crops"],
                "regne": row["regne"],
                "confiance": row["confiance"]
            }
        })

    # Import tasks
    client.projects.import_tasks(
        id=project_id,
        request=tasks,
        return_task_ids=True,
    )
    LOGGER.info("The tasks have been successfully imported ; number of tasks :", value=len(df))

def push_tasks_label_studio_no_crops(project_title: str, df: pl.DataFrame):
    api_key = os.getenv("LABEL_STUDIO_API_KEY_DATAFORGOOD")
    url = os.getenv("LABEL_STUDIO_URL")

    client = LabelStudio(base_url=url, api_key=api_key)
    projects = client.projects.list()

    project_id = None
    for project in projects:
        if project.title == project_title:
            project_id = project.id
            LOGGER.info(f"Projet ID={project.id}, Nom={project.title} exists")
            break

    if project_id is None:
        LOGGER.info(f"The project {project_title} does not exist.")
        return

    tasks = []

    for row in df.to_dicts():

        tasks.append({
            "data": {
                "image": {row["path_s3"]},
                "id_observation": row["id_observation"]
            }
        })

    # Import tasks
    client.projects.import_tasks(
        id=project_id,
        request=tasks,
        return_task_ids=True,
    )
    LOGGER.info("The tasks have been successfully imported ; number of tasks :", value=len(df))


def delete_tasks_label_studio(project_title: str):
    api_key = os.getenv("LABEL_STUDIO_API_KEY")
    url = "http://label-studio:8080"

    client = LabelStudio(base_url=url, api_key=api_key)
    projects = client.projects.list()
    project_id = None

    for project in projects:
        if project.title == project_title:
            project_id = project.id
            LOGGER.info(f"Projet ID={project.id}, Nom={project.title} exists")
            break

    if project_id is None:
        LOGGER.info(f"The project {project_title} does not exist.")
        return

    tasks = client.tasks.list(project=project.id)
    if not tasks:
        LOGGER.info("No tasks to delete.")
        return

    task_ids = [t.id for t in tasks]
    for task_id in task_ids:
        client.tasks.delete(task_id)
    LOGGER.info(f"{len(task_ids)} tasks deleted from project {project.id}")


def extracts_finished_tasks_label_studio(
        project_title: str, 
        date_min: datetime, date_max: datetime,
        path: str
):
    api_key = os.getenv("LABEL_STUDIO_API_KEY_DATAFORGOOD")
    url = os.getenv("LABEL_STUDIO_URL")

    client = LabelStudio(base_url=url, api_key=api_key)

    project = [
        p for p in client.projects.list() if p.title == project_title
    ][0]

    # Selects only the tasks that were completed over a given day/week/...
    filters = Filters.create(Filters.OR, [
        Filters.item(
            Column.completed_at, Operator.IN, Type.Datetime,
            Filters.value(date_min, date_max)
        )
    ])

    view = client.views.create(data=filters, project=project.id)
    export_job = client.projects.exports.create(
        id = project.id, task_filter_options = {"view": view.id}
    )
    export = client.projects.exports.get(
        id=project.id, export_pk=export_job.id
    )

    with open(path, "wb") as f:
        for chunk in client.projects.exports.download(
            id=project.id,
            export_pk=export.id,
            export_type="JSON",
            request_options={"chunk_size": 1024},
        ):
            f.write(chunk)