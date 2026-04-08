import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SQL_DIR = Path(__file__).parent / "lib"

PROJECT = os.environ.get("GCP_PROJECT_ID", "nba-data-engineering")
DATASET = os.environ.get("BQ_DATASET", "nba")

SEASON_START = datetime(2024, 10, 22).date()
SEASON_END = datetime(2025, 6, 22).date()


def _load_staging() -> None:
    from pipelines.load.player_team_scd_staging import load_staging
    load_staging()


def _advance_simulated_date() -> None:
    current = Variable.get("player_team_scd_simulated_date", default_var=str(SEASON_START))
    current_date = datetime.strptime(current, "%Y-%m-%d").date()
    next_date = min(current_date + timedelta(days=1), SEASON_END)
    Variable.set("player_team_scd_simulated_date", str(next_date))
    print(f"Advanced simulated date from {current_date} to {next_date}")


with DAG(
    dag_id="player_team_scd",
    description="SCD2: track player team changes over time",
    schedule="@hourly",
    start_date=datetime(2024, 10, 22),
    catchup=False,
    tags=["nba", "dimensions", "scd"],
) as dag:

    load_staging_task = PythonOperator(
        task_id="load_staging",
        python_callable=_load_staging,
    )

    merge_scd = BigQueryInsertJobOperator(
        task_id="merge_scd",
        configuration={
            "query": {
                "query": (SQL_DIR / "player_team_scd_merge.sql").read_text(),
                "useLegacySql": False,
            }
        },
        params={
            "project": PROJECT,
            "dataset": DATASET,
        },
    )

    advance_date = PythonOperator(
        task_id="advance_simulated_date",
        python_callable=_advance_simulated_date,
    )

    load_staging_task >> merge_scd >> advance_date
