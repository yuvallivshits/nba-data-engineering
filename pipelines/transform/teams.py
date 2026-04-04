from pipelines.utils.parquet_io import read_parquet, write_parquet
import os
import pandas as pd

def transform() -> None:
    teams_df = read_parquet(f"{os.environ['BRONZE_PATH']}/teams/data.parquet")
    teams_df['loaded_at'] = pd.Timestamp.now("UTC")
    teams_df = teams_df.rename(columns={'id': 'team_id',"loaded_at": "updated_at"})
    write_parquet(teams_df, f"{os.environ['SILVER_PATH']}/teams/data.parquet")

if __name__ == "__main__":
    transform()