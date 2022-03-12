import pendulum
import pytest
from airflow import DAG


@pytest.fixture
def test_dag() -> DAG:
    return DAG(dag_id="test_dag", start_date=pendulum.now())
