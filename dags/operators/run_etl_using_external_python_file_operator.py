from airflow.models import BaseOperator
from airflow.models.taskinstance import Context

from configs.sources import SOURCES
from operators.extract_operator import ExtractOperator
from operators.load_operator import LoadOperator
from operators.transform_operator import TransformOperator


class RunEtlUsingExternalPythonFileOperator(BaseOperator):
    def execute(self, context: Context):
        if len(SOURCES) <= 0:
            self.log.info("No sources found")

        for source in SOURCES:
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
