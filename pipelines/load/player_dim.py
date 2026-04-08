import os

from google.cloud import bigquery

from pipelines.utils.bigquery_client import get_client, get_dataset_ref
from pipelines.utils.parquet_io import read_parquet


def load_player_dim() -> None:
    client = get_client()
    table_ref = f"{get_dataset_ref()}.player_dim"

    player_df = read_parquet(f"{os.environ['SILVER_PATH']}/players/data.parquet")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(player_df, table_ref, job_config=job_config).result()
    print(f"Loaded {len(player_df)} rows into {table_ref}")


if __name__ == "__main__":
    load_player_dim()