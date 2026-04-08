import os
from datetime import datetime
from pathlib import Path

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG

SQL_DIR = Path(__file__).parent / "lib"

PROJECT = os.environ.get("GCP_PROJECT_ID", "nba-data-engineering")
DATASET = os.environ.get("BQ_DATASET", "nba")


with DAG(
    dag_id="player_dim",
    description="SCD1: update player current team after trades",
    schedule="@hourly",
    start_date=datetime(2024, 10, 22),
    catchup=False,
    tags=["nba", "dimensions"],
) as dag:

    wait_for_scd = ExternalTaskSensor(
        task_id="wait_for_player_team_scd",
        external_dag_id="player_team_scd",
        external_task_id="merge_scd",
        timeout=3600,
        mode="reschedule",
    )

    update_player_dim = BigQueryInsertJobOperator(
        task_id="update_player_dim",
        configuration={
            "query": {
                "query": (SQL_DIR / "player_dim_update.sql").read_text(),
                "useLegacySql": False,
            }
        },
        params={
            "project": PROJECT,
            "dataset": DATASET,
        },
    )

    wait_for_scd >> update_player_dim
