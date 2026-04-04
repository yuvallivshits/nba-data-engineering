import pandas as pd
from google.cloud import bigquery

from pipelines.utils.bigquery_client import get_client, get_dataset_ref
from pipelines.utils.dates import date_to_int, season_date_range


def generate_and_load(season: str = "2024-25") -> None:
    """
    Generate one row per calendar day for the season and load into BigQuery date_dim.

    Idempotent: deletes existing rows for the season before inserting,
    so re-running produces the same result without duplicates.
    """
    start_date, end_date = season_date_range(season)
    dates = pd.date_range(start=start_date, end=end_date)

    df = pd.DataFrame({
        "date_key":    [date_to_int(d.date()) for d in dates],
        "full_date":   [d.date() for d in dates],
        "day_of_week": [d.weekday() for d in dates],          # 0=Monday, 6=Sunday
        "day_name":    [d.strftime("%A") for d in dates],     # "Monday", "Tuesday"...
        "day_of_month":[d.day for d in dates],
        "month":       [d.month for d in dates],
        "month_name":  [d.strftime("%B") for d in dates],     # "October", "November"...
        "quarter":     [(d.month - 1) // 3 + 1 for d in dates],
        "year":        [d.year for d in dates],
        "is_weekend":  [d.weekday() >= 5 for d in dates],     # 5=Saturday, 6=Sunday
        "season":      season,
    })

    client = get_client()
    table_ref = f"{get_dataset_ref()}.date_dim"

    # WRITE_TRUNCATE replaces the entire table on each run — idempotent.
    # Safe for date_dim because it's a static calendar table, always fully regenerated.
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    print(f"Loaded {len(df)} rows into {table_ref}")