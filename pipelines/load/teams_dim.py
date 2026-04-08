import os

from google.cloud import bigquery

from pipelines.utils.bigquery_client import get_client, get_dataset_ref, get_table_schema
from pipelines.utils.parquet_io import read_parquet

def load_teams_dim() -> None:
    """
    Load teams_dim from Silver to BigQuery.

    Idempotent: deletes existing rows for the season before inserting,
    so re-running produces the same result without duplicates.
    """
    client = get_client()
    dataset_ref = get_dataset_ref()
    table_ref = f"{dataset_ref}.team_dim"

    teams_df = read_parquet(f"{os.environ['SILVER_PATH']}/teams/data.parquet")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    expected_columns = {field.name for field in get_table_schema("team_dim")}
    actual_columns = set(teams_df.columns)
    if actual_columns != expected_columns:
        raise ValueError(f"Schema mismatch. Missing: {expected_columns - actual_columns}, Extra: {actual_columns - expected_columns}")


    client.load_table_from_dataframe(teams_df, table_ref, job_config=job_config).result()
    print(f"Loaded {len(teams_df)} rows into {table_ref}")

if __name__ == "__main__":
    load_teams_dim()