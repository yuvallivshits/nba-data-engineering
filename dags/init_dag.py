from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SEASON = "2024-25"


def _generate_date_dim() -> None:
    from pipelines.load.date_dim import generate_and_load

    generate_and_load(season=SEASON)


with DAG(
    dag_id="init_dag",
    description="One-time initialization: generate date_dim",
    schedule=None,       # manual trigger only — never runs automatically
    start_date=datetime(2024, 10, 1),
    catchup=False,
    tags=["nba", "init"],
) as dag:

    generate_date_dim = PythonOperator(
        task_id="generate_date_dim",
        python_callable=_generate_date_dim
    )


