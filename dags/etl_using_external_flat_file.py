from pathlib import Path

import pendulum
import yaml
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from constants import SOURCE_CONFIG_FILE_PATH
from operators.extract_operator import ExtractOperator
from operators.load_operator import LoadOperator
from operators.transform_operator import TransformOperator

DAG_ID = "etl_using_external_flat_file"

SOURCES = "sources"


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.now(tz="Asia/Singapore"),
    schedule_interval=None,
)
def create_dag():
    split_files_by_source = DummyOperator(task_id="split_files_by_source")

    source_config_file_path = Path(SOURCE_CONFIG_FILE_PATH)

    sources = []

    if source_config_file_path.exists():
        with open(source_config_file_path, "r") as config_file:
            sources_config = yaml.safe_load(config_file)
        sources = sources_config.get(SOURCES, [])

    for source in sources:
        with TaskGroup(group_id=source) as task_group:
            extract = ExtractOperator(task_id="extract", source=source)

            transform = TransformOperator(task_id="transform", source=source)

            load = LoadOperator(task_id="load", source=source)

            extract >> transform >> load

        split_files_by_source >> task_group


globals()[DAG_ID] = create_dag()
