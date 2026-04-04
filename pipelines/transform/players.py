import os

import pandas as pd

from pipelines.utils.parquet_io import read_parquet, write_parquet


def transform() -> None:
    players_df = read_parquet(f"{os.environ['BRONZE_PATH']}/players/data.parquet")
    players_df['loaded_at'] = pd.Timestamp.now("UTC")
    write_parquet(players_df, f"{os.environ['SILVER_PATH']}/players/data.parquet")


if __name__ == "__main__":
    transform()
