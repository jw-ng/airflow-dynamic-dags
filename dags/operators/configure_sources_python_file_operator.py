from pathlib import Path

from airflow.models import BaseOperator
from airflow.models.taskinstance import Context


SOURCES_KEY = "sources"


class ConfigureSourcesPythonFileOperator(BaseOperator):
    def __init__(
        self,
        source_module_root_dir_path: str,
        sources_module: str,
        sources_attribute_name: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.source_module_root_dir_path = source_module_root_dir_path
        self.sources_module = sources_module
        self.sources_attribute_name = sources_attribute_name

    def execute(self, context: Context):
        if context["dag_run"] is None:
            self.log.info("No DAG run config given. No change to sources Python file")
            return

        run_config = context["dag_run"].conf

        if run_config.get(SOURCES_KEY) is None:
            self.log.warning(
                f"DAG run config JSON does not contain the required key '{SOURCES_KEY}'"
            )
            return

        source_module_dir_path = Path(self.source_module_root_dir_path)

        (*sub_modules, module_file_name) = self.sources_module.split(".")

        for sub_module in sub_modules:
            source_module_dir_path /= sub_module

        sources_python_file_path = source_module_dir_path / f"{module_file_name}.py"

        with open(sources_python_file_path, "w") as config_file:
            sources = run_config.get(SOURCES_KEY)
            config_file.writelines([f"SOURCES = {sources}"])
