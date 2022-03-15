from configure_sources_python_file_dag import create_dag
from constants import DAG_DIR, SOURCES_ATTRIBUTE_NAME, SOURCES_MODULE
from operators.configure_sources_python_file_operator import (
    ConfigureSourcesPythonFileOperator,
)


class TestConfigureSourcesPythonFileDag:
    def test_should_configure_sources_python_file(self):
        dag = create_dag()
        configure_sources_python_file_task: ConfigureSourcesPythonFileOperator = dag.tasks[0]

        assert configure_sources_python_file_task.task_id == "configure_sources_python_file"
        assert (
            type(configure_sources_python_file_task) is ConfigureSourcesPythonFileOperator
        )
        assert configure_sources_python_file_task.source_module_root_dir_path == str(
            DAG_DIR
        )
        assert configure_sources_python_file_task.sources_module == SOURCES_MODULE
        assert configure_sources_python_file_task.sources_attribute_name == SOURCES_ATTRIBUTE_NAME
