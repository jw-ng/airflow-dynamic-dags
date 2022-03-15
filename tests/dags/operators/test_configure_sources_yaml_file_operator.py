import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call

import pendulum
import pytest
import yaml
from airflow.models import TaskInstance
from airflow.models.taskinstance import Context

from operators.configure_sources_yaml_file_operator import (
    ConfigureSourcesYamlFileOperator,
)


class TestConfigureSourcesYamlFileOperator:
    def test_should_not_write_to_sources_yaml_file_when_no_config_json_specified_in_context(
        self, test_dag, spy_yaml_safe_dump
    ):
        operator = ConfigureSourcesYamlFileOperator(
            task_id="configure_sources_yaml_file",
            sources_yaml_file_path="some/file/path",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()

        operator.execute(test_context)

        assert spy_yaml_safe_dump.call_count == 0

    def test_should_log_to_info_when_no_config_json_specified_in_context(
            self, test_dag, spy_log
    ):
        operator = ConfigureSourcesYamlFileOperator(
            task_id="configure_sources_yaml_file",
            sources_yaml_file_path="some/file/path",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()

        operator.execute(test_context)

        assert spy_log.info.call_count == 1
        assert spy_log.info.call_args == call("No DAG run config given. No change to sources YAML file")

    @pytest.mark.parametrize(
        "run_config",
        [
            {"sources": []},
            {"sources": ["foo"]},
            {"sources": ["foo", "bar"]},
        ],
    )
    def test_should_write_sources_yaml_file_with_config_specified_in_context_when_sources_yaml_file_does_not_exists_yet(
        self, test_dag, run_config
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            sources_yaml_file_path = Path(temp_dir) / "sources.yaml"
            sources_yaml_file_path.unlink(missing_ok=True)

            operator = ConfigureSourcesYamlFileOperator(
                task_id="configure_sources_yaml_file",
                sources_yaml_file_path=str(sources_yaml_file_path),
                dag=test_dag,
            )

            ti = TaskInstance(task=operator, execution_date=pendulum.now())
            test_context: Context = ti.get_template_context()
            mock_dag_run = MagicMock()
            mock_dag_run.conf = run_config
            test_context["dag_run"] = mock_dag_run

            operator.execute(test_context)

            with open(sources_yaml_file_path, "r") as f:
                config = yaml.safe_load(f)

            assert config == run_config

    def test_should_not_write_to_sources_yaml_file_when_dag_run_config_does_not_contain_sources_key(
        self, test_dag, spy_yaml_safe_dump
    ):
        operator = ConfigureSourcesYamlFileOperator(
            task_id="configure_sources_yaml_file",
            sources_yaml_file_path="some/file/path",
            dag=test_dag,
        )

        ti = TaskInstance(task=operator, execution_date=pendulum.now())
        test_context: Context = ti.get_template_context()
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {"something": "else"}
        test_context["dag_run"] = mock_dag_run

        operator.execute(test_context)

        assert spy_yaml_safe_dump.call_count == 0

    def test_should_warn_about_run_misconfig_when_dag_run_config_does_not_contain_sources_key(
        self, test_dag, spy_log
    ):
        operator = ConfigureSourcesYamlFileOperator(
            task_id="configure_sources_yaml_file",
            sources_yaml_file_path="some/file/path",
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

    def test_should_overwrite_sources_yaml_file_with_config_specified_in_context_when_sources_yaml_file_exists(
        self, test_dag
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            sources_yaml_file_path = Path(temp_dir) / "sources.yaml"
            with open(sources_yaml_file_path, "w") as file:
                yaml.safe_dump({"sources": ["lorem", "ipsum"]}, file)

            operator = ConfigureSourcesYamlFileOperator(
                task_id="configure_sources_yaml_file",
                sources_yaml_file_path=str(sources_yaml_file_path),
                dag=test_dag,
            )

            ti = TaskInstance(task=operator, execution_date=pendulum.now())
            test_context: Context = ti.get_template_context()
            mock_dag_run = MagicMock()
            mock_dag_run.conf = {"sources": ["foo", "bar", "baz"]}
            test_context["dag_run"] = mock_dag_run

            operator.execute(test_context)

            with open(sources_yaml_file_path, "r") as f:
                config = yaml.safe_load(f)

            assert config == {"sources": ["foo", "bar", "baz"]}

    @pytest.fixture
    def spy_yaml_safe_dump(self, mocker) -> MagicMock:
        return mocker.patch(
            "operators.configure_sources_yaml_file_operator.yaml.safe_load"
        )

    @pytest.fixture
    def spy_log(self, mocker) -> MagicMock:
        return mocker.patch(
            "operators.configure_sources_yaml_file_operator.ConfigureSourcesYamlFileOperator.log"
        )
