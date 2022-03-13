from unittest.mock import call

from etl_using_operator_reading_external_python_file import create_dag


class TestEtlUsingOperatorReadingFromExternalPythonFile:
    def test_dag_should_split_files_by_source_first(self):
        dag = create_dag()

        assert dag.tasks[0].task_id == "split_files_by_source"

    def test_dag_should_run_etl_using_operator_that_reads_from_airflow_variables_after_split(
        self,
    ):
        dag = create_dag()

        [split_files_by_source, run_etl_for_each_source] = dag.tasks

        assert split_files_by_source.task_id == "split_files_by_source"
        assert run_etl_for_each_source.task_id == "run_etl_for_each_source"
        assert split_files_by_source.downstream_task_ids == {
            run_etl_for_each_source.task_id
        }

    def test_dag_should_initialise_run_etl_using_airflow_variable_operator_with_correct_source_variable_name(
        self, mocker
    ):
        spy_operator = mocker.patch(
            "etl_using_operator_reading_external_python_file.RunEtlUsingExternalPythonFileOperator"
        )

        create_dag()

        assert spy_operator.call_count == 1
        assert spy_operator.call_args == call(task_id="run_etl_for_each_source")
