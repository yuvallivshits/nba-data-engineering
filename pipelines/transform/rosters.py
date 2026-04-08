import os

import pandas as pd

from pipelines.utils.parquet_io import read_parquet, write_parquet



def transform() -> None:
    df = read_parquet(f"{os.environ['BRONZE_PATH']}/rosters/data.parquet")

    trades_df = df[df["HOW_ACQUIRED"].str.startswith("Traded from", na=False)].copy()
    trades_df["trade_date"] = pd.to_datetime(
        trades_df["HOW_ACQUIRED"].str.extract(r"on (\d{2}/\d{2}/\d{2})")[0],
        format="%m/%d/%y"
    )
    trades_df = trades_df[trades_df["trade_date"] >= "2024-10-01"].copy()
    trades_df["from_team"] = trades_df["HOW_ACQUIRED"].str.extract(r"Traded from (\w+) on")
    trades_df = trades_df.rename(columns={"PLAYER_ID": "player_id"})

    teams_df = read_parquet(f"{os.environ['SILVER_PATH']}/teams/data.parquet")
    teams_lookup = dict(zip(teams_df["abbreviation"], teams_df["team_id"], strict=False))
    trades_df["from_team_id"] = trades_df["from_team"].map(teams_lookup)

    trades_df = trades_df[["player_id", "team_id", "from_team_id", "trade_date"]]

    write_parquet(trades_df, f"{os.environ['SILVER_PATH']}/rosters/data.parquet")
    print(f"Transformed {len(trades_df)} in-season trades.")



if __name__ == "__main__":
    transform()
