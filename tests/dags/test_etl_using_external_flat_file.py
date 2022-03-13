from unittest.mock import mock_open

import pytest

from etl_using_external_flat_file import create_dag


class TestEtlUsingExternalFlatFile:
    def test_dag_should_split_files_by_source_first(self, mock_sources):
        dag = create_dag()

        assert dag.tasks[0].task_id == "split_files_by_source"

    def test_dag_should_create_one_etl_task_group_per_sources_specified_in_external_python_file(
        self,
        mock_sources,
    ):
        dag = create_dag()

        for source in ["foo", "bar", "baz"]:
            extract_task = dag.get_task(f"{source}.extract")
            transform_task = dag.get_task(f"{source}.transform")
            load_task = dag.get_task(f"{source}.load")
            assert extract_task.downstream_task_ids == {transform_task.task_id}
            assert transform_task.downstream_task_ids == {load_task.task_id}

    def test_dag_should_create_no_etl_task_groups_when_flat_file_is_not_found(
        self,
        flat_file_not_found,
    ):
        dag = create_dag()

        assert dag.task_ids == ["split_files_by_source"]

    @pytest.fixture
    def mock_sources(self, mocker):
        mock_data = """
        sources:
         - foo
         - bar
         - baz
        """
        mocker.patch("builtins.open", mock_open(read_data=mock_data))

    @pytest.fixture
    def flat_file_not_found(self, mocker):
        mocker.patch("etl_using_external_flat_file.Path.exists", return_value=False)
