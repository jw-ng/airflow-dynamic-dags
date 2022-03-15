import pendulum
from airflow.decorators import dag

from constants import SOURCE_CONFIG_FILE_PATH
from operators.configure_sources_yaml_file_operator import ConfigureSourcesYamlFileOperator

DAG_ID = "configure_sources_yaml_file_dag"


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.now(tz="Asia/Singapore"),
    schedule_interval=None,
)
def create_dag():
    ConfigureSourcesYamlFileOperator(
        task_id="configure_sources_yaml_file",
        sources_yaml_file_path=str(SOURCE_CONFIG_FILE_PATH),
    )


globals()[DAG_ID] = create_dag()
