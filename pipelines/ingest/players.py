import os

from nba_api.stats.endpoints import playerindex

from pipelines.utils.parquet_io import write_parquet


def ingest() -> None:
    df = playerindex.PlayerIndex(season='2024-25', historical_nullable=1, timeout=30).get_data_frames()[0]
    write_parquet(df, f"{os.environ['BRONZE_PATH']}/players/data.parquet")


if __name__ == "__main__":
    ingest()