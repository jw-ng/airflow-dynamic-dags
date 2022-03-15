import yaml
from airflow.models import BaseOperator
from airflow.models.taskinstance import Context


SOURCES_KEY = "sources"


class ConfigureSourcesYamlFileOperator(BaseOperator):
    def __init__(self, sources_yaml_file_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.sources_yaml_file_path = sources_yaml_file_path

    def execute(self, context: Context):
        if context["dag_run"] is None:
            self.log.info("No DAG run config given. No change to sources YAML file")
            return

        run_config = context["dag_run"].conf

        if run_config.get(SOURCES_KEY) is None:
            self.log.warning(
                f"DAG run config JSON does not contain the required key '{SOURCES_KEY}'"
            )
            return

        with open(self.sources_yaml_file_path, "w") as config_file:
            yaml.safe_dump(run_config, config_file)
