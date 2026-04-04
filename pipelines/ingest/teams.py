import os
import pandas as pd
from pipelines.utils.parquet_io import write_parquet
from nba_api.stats.static import teams

def ingest() -> None:
    teams_list = teams.get_teams()
    teams_df = pd.DataFrame(teams_list)
    write_parquet(teams_df, f"{os.environ['BRONZE_PATH']}/teams/data.parquet")


if __name__ == "__main__":
    ingest()
