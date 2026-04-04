import os
import pandas as pd
from pipelines.utils.parquet_io import write_parquet
from nba_api.stats.endpoints import playerindex
    
def ingest() -> None:
    df = playerindex.PlayerIndex(season='2024-25', timeout=30).get_data_frames()[0]
    write_parquet(df, f"{os.environ['BRONZE_PATH']}/players/data.parquet")


if __name__ == "__main__":
    ingest()