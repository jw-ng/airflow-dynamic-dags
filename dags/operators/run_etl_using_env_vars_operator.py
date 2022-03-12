from airflow.models import BaseOperator
from airflow.models.taskinstance import Context

from operators.extract_operator import ExtractOperator
from operators.load_operator import LoadOperator
from operators.transform_operator import TransformOperator
from utils.source_config.env_var import get_sources


class RunEtlUsingEnvVarsOperator(BaseOperator):
    def execute(self, context: Context):
        sources = get_sources()

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
