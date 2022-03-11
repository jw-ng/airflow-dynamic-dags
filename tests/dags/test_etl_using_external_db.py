import pytest

from etl_using_external_db import create_dag


class TestEtlUsingExternalDb:
    def test_dag_should_split_files_by_source_first(self, mock_get_sources):
        dag = create_dag()

        assert dag.tasks[0].task_id == "split_files_by_source"

    def test_dag_should_create_one_etl_task_group_per_sources_specified_in_external_db(
        self, mock_get_sources
    ):
        dag = create_dag()

        for source in ["foo", "bar", "baz"]:
            extract_task = dag.get_task(f"{source}.extract")
            transform_task = dag.get_task(f"{source}.transform")
            load_task = dag.get_task(f"{source}.load")
            assert extract_task.downstream_task_ids == {transform_task.task_id}
            assert transform_task.downstream_task_ids == {load_task.task_id}

    @pytest.fixture
    def mock_get_sources(self, mocker):
        mocker.patch(
            "etl_using_external_db.get_sources",
            return_value=["foo", "bar", "baz"],
        )
