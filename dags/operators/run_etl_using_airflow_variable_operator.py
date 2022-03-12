from airflow.models import BaseOperator, Variable
from airflow.models.taskinstance import Context

from operators.extract_operator import ExtractOperator
from operators.load_operator import LoadOperator
from operators.transform_operator import TransformOperator


class RunEtlUsingAirflowVariableOperator(BaseOperator):
    def __init__(self, sources_var_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.sources_var_name = sources_var_name

    def execute(self, context: Context):
        sources = Variable.get(
            self.sources_var_name,
            default_var=[],
            deserialize_json=True,
        )

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
