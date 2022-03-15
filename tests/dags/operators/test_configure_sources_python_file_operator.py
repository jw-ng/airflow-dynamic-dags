import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open

import pendulum
import pytest
from airflow.models import TaskInstance
from airflow.models.taskinstance import Context

from operators.configure_sources_python_file_operator import (
    ConfigureSourcesPythonFileOperator,
)


class TestConfigureSourcesPythonFileOperator:
    def test_should_not_write_to_sources_python_file_when_no_config_json_specified_in_context(
        self, test_dag, mock_config_file_open
    ):
        operator = ConfigureSourcesPythonFileOperator(
            task_id="configure_sources_python_file",
            source_module_root_dir_path="some/root/dir/path",
            sources_module="module.submodule.sources",
            sources_attribute_name="SOURCES",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()

        operator.execute(test_context)

        assert mock_config_file_open.call_count == 0

    def test_should_log_to_info_when_no_config_json_specified_in_context(
        self, test_dag, spy_log
    ):
        operator = ConfigureSourcesPythonFileOperator(
            task_id="configure_sources_python_file",
            source_module_root_dir_path="some/root/dir/path",
            sources_module="module.submodule.sources",
            sources_attribute_name="SOURCES",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()

        operator.execute(test_context)

        assert spy_log.info.call_count == 1
        assert spy_log.info.call_args == call(
            "No DAG run config given. No change to sources Python file"
        )

    def test_should_write_to_specified_sources_python_file_path(
        self, test_dag, mock_config_file_open
    ):
        operator = ConfigureSourcesPythonFileOperator(
            task_id="configure_sources_python_file",
            source_module_root_dir_path="some/root/dir/path",
            sources_module="module.submodule.sources",
            sources_attribute_name="SOURCES",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {"sources": []}
        test_context["dag_run"] = mock_dag_run

        operator.execute(test_context)

        assert mock_config_file_open.call_count == 1
        assert mock_config_file_open.call_args == call(
            Path("some/root/dir/path/module/submodule/sources.py"), "w"
        )

    @pytest.mark.parametrize(
        "run_config, expected_content",
        [
            ({"sources": []}, "SOURCES = []"),
            ({"sources": ["foo"]}, "SOURCES = ['foo']"),
            ({"sources": ["foo", "bar"]}, "SOURCES = ['foo', 'bar']"),
        ],
    )
    def test_should_write_sources_python_file_with_config_specified_in_context_when_sources_python_file_does_not_exists_yet(
        self, test_dag, mock_config_file_open, run_config, expected_content
    ):
        operator = ConfigureSourcesPythonFileOperator(
            task_id="configure_sources_python_file",
            source_module_root_dir_path="some/root/dir/path",
            sources_module="module.submodule.sources",
            sources_attribute_name="SOURCES",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()
        mock_dag_run = MagicMock()
        mock_dag_run.conf = run_config
        test_context["dag_run"] = mock_dag_run

        operator.execute(test_context)

        config_file_handle = mock_config_file_open()
        assert config_file_handle.writelines.call_count == 1
        assert config_file_handle.writelines.call_args == call([expected_content])

    def test_should_not_write_to_sources_python_file_when_dag_run_config_does_not_contain_sources_key(
        self, test_dag, mock_config_file_open
    ):
        operator = ConfigureSourcesPythonFileOperator(
            task_id="configure_sources_python_file",
            source_module_root_dir_path="some/root/dir/path",
            sources_module="module.submodule.sources",
            sources_attribute_name="SOURCES",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {"something": "else"}
        test_context["dag_run"] = mock_dag_run

        operator.execute(test_context)

        assert mock_config_file_open.call_count == 0

    def test_should_warn_about_run_misconfig_when_dag_run_config_does_not_contain_sources_key(
        self, test_dag, spy_log
    ):
        operator = ConfigureSourcesPythonFileOperator(
            task_id="configure_sources_python_file",
            source_module_root_dir_path="some/root/dir/path",
            sources_module="module.submodule.sources",
            sources_attribute_name="SOURCES",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {"something": "else"}
        test_context["dag_run"] = mock_dag_run

        operator.execute(test_context)

        assert spy_log.warning.call_count == 1
        assert spy_log.warning.call_args == call(
            "DAG run config JSON does not contain the required key 'sources'"
        )

    def test_should_overwrite_sources_python_file_with_config_specified_in_context_when_sources_python_file_exists(
        self, test_dag
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            sources_dir_path = Path(temp_dir) / "configs"
            sources_dir_path.mkdir(parents=True)
            sources_python_file_path = sources_dir_path / "sources.py"
            with sources_python_file_path.open("w") as file:
                file.write("SOURCES = []")

            operator = ConfigureSourcesPythonFileOperator(
                task_id="configure_sources_python_file",
                source_module_root_dir_path=temp_dir,
                sources_module="configs.sources",
                sources_attribute_name="SOURCES",
                dag=test_dag,
            )

            ti = TaskInstance(task=operator, execution_date=pendulum.now())
            test_context: Context = ti.get_template_context()
            mock_dag_run = MagicMock()
            mock_dag_run.conf = {"sources": ["foo", "bar", "baz"]}
            test_context["dag_run"] = mock_dag_run

            operator.execute(test_context)

            with open(sources_python_file_path, "r") as f:
                content = f.read()

            assert content == "SOURCES = ['foo', 'bar', 'baz']"

    @pytest.fixture
    def mock_config_file_open(self, mocker) -> MagicMock:
        mock_config_file_open = mock_open()
        mocker.patch("builtins.open", mock_config_file_open)
        return mock_config_file_open

    @pytest.fixture
    def spy_log(self, mocker) -> MagicMock:
        return mocker.patch(
            "operators.configure_sources_python_file_operator.ConfigureSourcesPythonFileOperator.log"
        )
