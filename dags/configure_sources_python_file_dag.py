import pendulum
from airflow.decorators import dag

from constants import DAG_DIR, SOURCES_ATTRIBUTE_NAME, SOURCES_MODULE
from operators.configure_sources_python_file_operator import ConfigureSourcesPythonFileOperator

DAG_ID = "configure_sources_python_file_dag"


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.now(tz="Asia/Singapore"),
    schedule_interval=None,
)
def create_dag():
    ConfigureSourcesPythonFileOperator(
        task_id="configure_sources_python_file",
        source_module_root_dir_path=str(DAG_DIR),
        sources_module=SOURCES_MODULE,
        sources_attribute_name=SOURCES_ATTRIBUTE_NAME,
    )


globals()[DAG_ID] = create_dag()
