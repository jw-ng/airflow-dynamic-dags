from pathlib import Path

import yaml
from airflow.models import BaseOperator
from airflow.models.taskinstance import Context

from operators.extract_operator import ExtractOperator
from operators.load_operator import LoadOperator
from operators.transform_operator import TransformOperator

DAG_DIR = Path(__file__).parent.parent
CONFIG_DIR = "configs"
SOURCES_FILE = "sources.yaml"

SOURCES = "sources"


class RunEtlUsingExternalFlatFileOperator(BaseOperator):
    def execute(self, context: Context):
        source_config_file_path = DAG_DIR / CONFIG_DIR / SOURCES_FILE
        sources = []

        if source_config_file_path.exists():
            with source_config_file_path.open("r") as config_file:
                sources_config = yaml.safe_load(config_file)
            sources = sources_config.get(SOURCES, [])

        if len(sources) <= 0:
            self.log.info("No sources found")

        for source in sources:
            ExtractOperator(
                task_id=f"extract_for_source_{source}",
                source=source,
            ).execute(context)

            TransformOperator(
                task_id=f"transform_for_source_{source}",
                source=source,
            ).execute(context)

            LoadOperator(
                task_id=f"load_for_source_{source}",
                source=source,
            ).execute(context)
