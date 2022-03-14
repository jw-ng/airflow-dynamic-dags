import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from operators.extract_operator import ExtractOperator
from operators.load_operator import LoadOperator
from operators.transform_operator import TransformOperator

DAG_ID = "etl_using_airflow_variables"
SOURCES_VAR_NAME = "sources"


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.now(tz="Asia/Singapore"),
    schedule_interval=None,
)
def create_dag():
    split_files_by_source = DummyOperator(task_id="split_files_by_source")

    sources = Variable.get(SOURCES_VAR_NAME, default_var=[], deserialize_json=True)

    for source in sources:
        with TaskGroup(group_id=source) as task_group:
            extract = ExtractOperator(task_id="extract", source=source)

            transform = TransformOperator(task_id="transform", source=source)

            load = LoadOperator(task_id="load", source=source)

            extract >> transform >> load

        split_files_by_source >> task_group


globals()[DAG_ID] = create_dag()
