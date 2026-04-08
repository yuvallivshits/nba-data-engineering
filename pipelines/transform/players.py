import os

import pandas as pd

from pipelines.utils.parquet_io import read_parquet, write_parquet


def transform() -> None:
    players_df = read_parquet(f"{os.environ['BRONZE_PATH']}/players/data.parquet")

   # only keep players active in the 2024-25 season (FROM_YEAR <= 2024 and TO_YEAR >= 2024)
    players_df["FROM_YEAR"] = pd.to_numeric(players_df["FROM_YEAR"], errors="coerce")
    players_df["TO_YEAR"] = pd.to_numeric(players_df["TO_YEAR"], errors="coerce")

   
    players_df = players_df[
    (players_df["FROM_YEAR"] <= 2024) & (players_df["TO_YEAR"] >= 2024)
]

    players_df["full_name"] = players_df["PLAYER_FIRST_NAME"] + " " + players_df["PLAYER_LAST_NAME"]

    players_df = players_df.rename(columns={
        "PERSON_ID": "player_id",
        "PLAYER_FIRST_NAME": "first_name",
        "PLAYER_LAST_NAME": "last_name",
        "JERSEY_NUMBER": "jersey_number",
        "POSITION": "position",
        "HEIGHT": "height",
        "WEIGHT": "weight",
        "COLLEGE": "college",
        "COUNTRY": "country",
        "DRAFT_YEAR": "draft_year",
        "DRAFT_ROUND": "draft_round",
        "DRAFT_NUMBER": "draft_number",
        "TEAM_ID": "team_id",
    })

    for col in ["draft_year", "draft_round", "draft_number"]:
        players_df[col] = pd.to_numeric(players_df[col], errors="coerce").astype("Int64")

    players_df["updated_at"] = pd.Timestamp.now("UTC")

    players_df = players_df[[
        "player_id", "full_name", "first_name", "last_name",
        "jersey_number", "position", "height", "weight",
        "college", "country", "draft_year", "draft_round", "draft_number",
        "team_id", "updated_at"
    ]]

    write_parquet(players_df, f"{os.environ['SILVER_PATH']}/players/data.parquet")


if __name__ == "__main__":
    transform()
