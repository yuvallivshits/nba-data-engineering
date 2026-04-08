import os
import time

import pandas as pd
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.static import teams

from pipelines.utils.parquet_io import write_parquet


def ingest() -> None:
    teams_list = teams.get_teams()
    all_rosters = []
    for team in teams_list:
        df = commonteamroster.CommonTeamRoster(
            team_id=team["id"], 
            season="2024-25", 
            timeout=30
        ).get_data_frames()[0]
        df["team_id"] = team["id"]
        all_rosters.append(df)
        time.sleep(1)
    
    rosters_df = pd.concat(all_rosters, ignore_index=True)
    write_parquet(rosters_df, f"{os.environ['BRONZE_PATH']}/rosters/data.parquet")
    print(f"Ingested rosters for {len(teams_list)} teams, total {len(rosters_df)} rows.")

if __name__ == "__main__":
    ingest()