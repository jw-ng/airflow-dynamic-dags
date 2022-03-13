import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator

from operators.run_etl_using_external_python_file_operator import RunEtlUsingExternalPythonFileOperator

DAG_ID = "etl_using_operator_reading_external_python_file"


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.now(tz="Asia/Singapore"),
    schedule_interval=None,
)
def create_dag():
    split_files_by_source = DummyOperator(task_id="split_files_by_source")

    run_etl_for_each_source = RunEtlUsingExternalPythonFileOperator(
        task_id="run_etl_for_each_source",
    )

    split_files_by_source >> run_etl_for_each_source


globals()[DAG_ID] = create_dag()
