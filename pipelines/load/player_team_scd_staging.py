import os

from google.cloud import bigquery

from pipelines.utils.bigquery_client import get_client, get_dataset_ref
from pipelines.utils.parquet_io import read_parquet


def load_staging() -> None:
    client = get_client()
    table_ref = f"{get_dataset_ref()}.player_team_scd_staging"

    try:
        result = client.query(f"SELECT COUNT(*) as cnt FROM `{table_ref}`").result()
        count = next(result)[0]
        if count > 0:
            print(f"Staging table already has {count} rows, skipping load.")
            return
    except Exception:
        pass  # table doesn't exist yet, proceed with load

    trades_df = read_parquet(f"{os.environ['SILVER_PATH']}/rosters/data.parquet")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(trades_df, table_ref, job_config=job_config).result()
    print(f"Loaded {len(trades_df)} rows into {table_ref}")


if __name__ == "__main__":
    load_staging()
