from configure_sources_yaml_file_dag import create_dag
from constants import SOURCE_CONFIG_FILE_PATH
from operators.configure_sources_yaml_file_operator import (
    ConfigureSourcesYamlFileOperator,
)


class TestConfigureSourcesYamlFileDag:
    def test_should_configure_sources_yaml_file(self):
        dag = create_dag()
        configure_sources_yaml_file_task = dag.tasks[0]

        assert configure_sources_yaml_file_task.task_id == "configure_sources_yaml_file"
        assert (
            type(configure_sources_yaml_file_task) is ConfigureSourcesYamlFileOperator
        )
        assert configure_sources_yaml_file_task.sources_yaml_file_path == str(
            SOURCE_CONFIG_FILE_PATH
        )
