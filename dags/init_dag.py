from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SEASON = "2024-25"


def _generate_date_dim() -> None:
    from pipelines.load.date_dim import generate_and_load
    generate_and_load(season=SEASON)


def _ingest_teams() -> None:
    from pipelines.ingest.teams import ingest
    ingest()


def _transform_teams() -> None:
    from pipelines.transform.teams import transform
    transform()


def _load_team_dim() -> None:
    from pipelines.load.teams_dim import load_teams_dim
    load_teams_dim()


with DAG(
    dag_id="init_dag",
    description="One-time initialization: date_dim, team_dim",
    schedule=None,
    start_date=datetime(2024, 10, 1),
    catchup=False,
    tags=["nba", "init"],
) as dag:

    generate_date_dim = PythonOperator(
        task_id="generate_date_dim",
        python_callable=_generate_date_dim,
    )

    ingest_teams = PythonOperator(
        task_id="ingest_teams",
        python_callable=_ingest_teams,
    )

    transform_teams = PythonOperator(
        task_id="transform_teams",
        python_callable=_transform_teams,
    )

    load_team_dim = PythonOperator(
        task_id="load_team_dim",
        python_callable=_load_team_dim,
    )

    ingest_teams >> transform_teams >> load_team_dim


