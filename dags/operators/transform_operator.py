from airflow.models import BaseOperator
from airflow.models.taskinstance import Context


class TransformOperator(BaseOperator):
    def __init__(self, source: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = source

    def execute(self, context: Context):
        self.log.info(f"Transforming for source {self.source}")
