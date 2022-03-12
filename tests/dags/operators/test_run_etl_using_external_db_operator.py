from unittest.mock import MagicMock, call

import pendulum
from airflow import DAG
from airflow.models import TaskInstance
from airflow.models.taskinstance import Context

from operators.run_etl_using_external_db_operator import RunEtlUsingExternalDbOperator


class TestRunEtlUsingExternalDbOperator:
    def test_should_create_etl_operators_for_each_source(self, mocker, test_dag: DAG):
        mocker.patch(
            "operators.run_etl_using_external_db_operator.get_sources",
            return_value=["foo", "bar", "baz"],
        )

        spy_extract_operator = mocker.patch(
            "operators.run_etl_using_external_db_operator.ExtractOperator"
        )
        spy_transform_operator = mocker.patch(
            "operators.run_etl_using_external_db_operator.TransformOperator"
        )
        spy_load_operator = mocker.patch(
            "operators.run_etl_using_external_db_operator.LoadOperator"
        )

        operator = RunEtlUsingExternalDbOperator(task_id="test_task", dag=test_dag)

        operator.execute({})

        assert spy_extract_operator.call_args_list == [
            call(task_id="extract_for_source_foo", source="foo"),
            call(task_id="extract_for_source_bar", source="bar"),
            call(task_id="extract_for_source_baz", source="baz"),
        ]
        assert spy_transform_operator.call_args_list == [
            call(task_id="transform_for_source_foo", source="foo"),
            call(task_id="transform_for_source_bar", source="bar"),
            call(task_id="transform_for_source_baz", source="baz"),
        ]
        assert spy_load_operator.call_args_list == [
            call(task_id="load_for_source_foo", source="foo"),
            call(task_id="load_for_source_bar", source="bar"),
            call(task_id="load_for_source_baz", source="baz"),
        ]

    def test_should_execute_etl_operators_using_context(self, mocker, test_dag: DAG):
        mocker.patch(
            "operators.run_etl_using_external_db_operator.get_sources",
            return_value=["foo"],
        )

        mock_extract_operator = MagicMock()
        mocker.patch(
            "operators.run_etl_using_external_db_operator.ExtractOperator",
            return_value=mock_extract_operator,
        )
        mock_transform_operator = MagicMock()
        mocker.patch(
            "operators.run_etl_using_external_db_operator.TransformOperator",
            return_value=mock_transform_operator,
        )
        mock_load_operator = MagicMock()
        mocker.patch(
            "operators.run_etl_using_external_db_operator.LoadOperator",
            return_value=mock_load_operator,
        )

        operator = RunEtlUsingExternalDbOperator(task_id="test_task", dag=test_dag)
        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()

        operator.execute(test_context)

        assert mock_extract_operator.execute.call_count == 1
        assert mock_extract_operator.execute.call_args == call(test_context)
        assert mock_transform_operator.execute.call_count == 1
        assert mock_transform_operator.execute.call_args == call(test_context)
        assert mock_load_operator.execute.call_count == 1
        assert mock_load_operator.execute.call_args == call(test_context)

    def test_should_log_info_message_when_sources_was_empty_list(
        self, mocker, test_dag: DAG
    ):
        spy_log = mocker.patch(
            "operators.run_etl_using_external_db_operator.RunEtlUsingExternalDbOperator.log"
        )
        mocker.patch(
            "operators.run_etl_using_external_db_operator.get_sources",
            return_value=[],
        )

        operator = RunEtlUsingExternalDbOperator(task_id="test_task", dag=test_dag)

        operator.execute({})

        assert spy_log.info.call_count == 1
        assert spy_log.info.call_args == call("No sources found")
