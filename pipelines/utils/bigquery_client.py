import os

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField


def get_client() -> bigquery.Client:
    """
    Create a BigQuery client using credentials from the environment.

    Reads GOOGLE_APPLICATION_CREDENTIALS and GCP_PROJECT_ID from env vars.
    These are set in .env and exported by start_airflow.sh.
    """
    project = os.environ["GCP_PROJECT_ID"]
    return bigquery.Client(project=project)


def get_dataset_ref() -> str:
    """
    Returns the fully qualified BigQuery dataset: project.dataset
    e.g. 'nba-data-engineering.nba'
    """
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get("BQ_DATASET", "nba")
    return f"{project}.{dataset}"


def init_dataset(client: bigquery.Client) -> None:
    """
    Create the BigQuery dataset if it doesn't exist.
    A dataset in BigQuery is like a schema/database in SQL — it's a container for tables.
    """
    dataset_id = get_dataset_ref()
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset ready: {dataset_id}")




def get_table_schema(table_name: str) -> list[SchemaField]:
    client = get_client()
    table_ref = f"{get_dataset_ref()}.{table_name}"
    table = client.get_table(table_ref)
    return list(table.schema)

def init_schema(client: bigquery.Client) -> None:
    """
    Create all gold layer tables in BigQuery if they don't already exist.
    Safe to run multiple times (idempotent) — uses exists_ok=True.
    """
    init_dataset(client)
    dataset = get_dataset_ref()
    tables = {
        # Calendar table — generated once for the 2024-25 season.
        # Used to filter/group by week, month, etc. in the dashboard.
        "date_dim": [
            SchemaField("date_key", "INTEGER", mode="REQUIRED"),   # YYYYMMDD
            SchemaField("full_date", "DATE", mode="REQUIRED"),
            SchemaField("day_of_week", "INTEGER", mode="REQUIRED"),  # 0=Monday
            SchemaField("day_name", "STRING", mode="REQUIRED"),
            SchemaField("day_of_month", "INTEGER", mode="REQUIRED"),
            SchemaField("month", "INTEGER", mode="REQUIRED"),
            SchemaField("month_name", "STRING", mode="REQUIRED"),
            SchemaField("quarter", "INTEGER", mode="REQUIRED"),
            SchemaField("year", "INTEGER", mode="REQUIRED"),
            SchemaField("is_weekend", "BOOLEAN", mode="REQUIRED"),
            SchemaField("season", "STRING", mode="REQUIRED"),       # '2024-25'
        ],

        # Static team info — SCD Type 1 (simple upsert, no history).
        "team_dim": [
            SchemaField("team_id", "INTEGER", mode="REQUIRED"),
            SchemaField("full_name", "STRING", mode="REQUIRED"),
            SchemaField("abbreviation", "STRING", mode="REQUIRED"),
            SchemaField("nickname", "STRING", mode="REQUIRED"),
            SchemaField("city", "STRING", mode="REQUIRED"),
            SchemaField("state", "STRING", mode="REQUIRED"),
            SchemaField("year_founded", "INTEGER", mode="NULLABLE"),
            SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        ],

        # Static player info — current team included (SCD1 on team_id).
        # Full trade history lives in player_team_scd.
        "player_dim": [
            SchemaField("player_id", "INTEGER", mode="REQUIRED"),
            SchemaField("full_name", "STRING", mode="REQUIRED"),
            SchemaField("first_name", "STRING", mode="REQUIRED"),
            SchemaField("last_name", "STRING", mode="REQUIRED"),
            SchemaField("team_id", "INTEGER", mode="NULLABLE"),
            SchemaField("jersey_number", "STRING", mode="NULLABLE"),
            SchemaField("position", "STRING", mode="NULLABLE"),
            SchemaField("height", "STRING", mode="NULLABLE"),
            SchemaField("weight", "STRING", mode="NULLABLE"),
            SchemaField("college", "STRING", mode="NULLABLE"),
            SchemaField("country", "STRING", mode="NULLABLE"),
            SchemaField("draft_year", "INTEGER", mode="NULLABLE"),
            SchemaField("draft_round", "INTEGER", mode="NULLABLE"),
            SchemaField("draft_number", "INTEGER", mode="NULLABLE"),
            SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        ],

        # SCD Type 2 — tracks which team a player plays for over time.
        # New row on every trade. To find a player's team on a specific game date:
        #   WHERE player_id = ? AND valid_from <= game_date AND valid_to >= game_date
        "player_team_scd": [
            SchemaField("player_id", "INTEGER", mode="REQUIRED"),
            SchemaField("team_id", "INTEGER", mode="REQUIRED"),
            SchemaField("valid_from", "DATE", mode="REQUIRED"),
            SchemaField("valid_to", "DATE", mode="REQUIRED"),   # '9999-12-31' = current
            SchemaField("is_current", "BOOLEAN", mode="REQUIRED"),
            SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
        ],

        # One row per game — raw scores only.
        # Winner derived at query time: home_score > away_score → home team won.
        "game_fact": [
            SchemaField("game_id", "STRING", mode="REQUIRED"),
            SchemaField("game_date", "DATE", mode="REQUIRED"),
            SchemaField("home_team_id", "INTEGER", mode="REQUIRED"),
            SchemaField("away_team_id", "INTEGER", mode="REQUIRED"),
            SchemaField("home_score", "INTEGER", mode="NULLABLE"),
            SchemaField("away_score", "INTEGER", mode="NULLABLE"),
            SchemaField("season", "STRING", mode="REQUIRED"),
            SchemaField("season_type", "STRING", mode="REQUIRED"),  # 'Regular Season'
            SchemaField("status", "STRING", mode="REQUIRED"),
            SchemaField("arena", "STRING", mode="NULLABLE"),
            SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ],

        # One row per player per game — all raw stats, no derived percentages.
        # FG%, 3P%, FT% computed at query time: fg_made / fg_attempted etc.
        "player_game_stats_fact": [
            SchemaField("game_id", "STRING", mode="REQUIRED"),
            SchemaField("player_id", "INTEGER", mode="REQUIRED"),
            SchemaField("game_date", "DATE", mode="REQUIRED"),
            SchemaField("minutes", "FLOAT", mode="NULLABLE"),
            SchemaField("points", "INTEGER", mode="NULLABLE"),
            SchemaField("rebounds_offensive", "INTEGER", mode="NULLABLE"),
            SchemaField("rebounds_defensive", "INTEGER", mode="NULLABLE"),
            SchemaField("assists", "INTEGER", mode="NULLABLE"),
            SchemaField("steals", "INTEGER", mode="NULLABLE"),
            SchemaField("blocks", "INTEGER", mode="NULLABLE"),
            SchemaField("turnovers", "INTEGER", mode="NULLABLE"),
            SchemaField("fouls", "INTEGER", mode="NULLABLE"),
            SchemaField("sportsmanship_fouls", "INTEGER", mode="NULLABLE"),
            SchemaField("is_ejected", "BOOLEAN", mode="NULLABLE"),
            SchemaField("fg_made", "INTEGER", mode="NULLABLE"),
            SchemaField("fg_attempted", "INTEGER", mode="NULLABLE"),
            SchemaField("fg3_made", "INTEGER", mode="NULLABLE"),
            SchemaField("fg3_attempted", "INTEGER", mode="NULLABLE"),
            SchemaField("ft_made", "INTEGER", mode="NULLABLE"),
            SchemaField("ft_attempted", "INTEGER", mode="NULLABLE"),
            SchemaField("plus_minus", "INTEGER", mode="NULLABLE"),
            SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ],

        # One row per team per day — full standings state captured daily.
        # Stored daily even on no-game days so the dashboard line chart has no gaps.
        # win_pct derived at query time: wins / (wins + losses)
        "standings_snapshot": [
            SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
            SchemaField("team_id", "INTEGER", mode="REQUIRED"),
            SchemaField("wins", "INTEGER", mode="REQUIRED"),
            SchemaField("losses", "INTEGER", mode="REQUIRED"),
            SchemaField("conference", "STRING", mode="REQUIRED"),
            SchemaField("division", "STRING", mode="REQUIRED"),
            SchemaField("conference_rank", "INTEGER", mode="NULLABLE"),
            SchemaField("division_rank", "INTEGER", mode="NULLABLE"),
            SchemaField("home_wins", "INTEGER", mode="NULLABLE"),
            SchemaField("home_losses", "INTEGER", mode="NULLABLE"),
            SchemaField("away_wins", "INTEGER", mode="NULLABLE"),
            SchemaField("away_losses", "INTEGER", mode="NULLABLE"),
            SchemaField("last_10_wins", "INTEGER", mode="NULLABLE"),
            SchemaField("last_10_losses", "INTEGER", mode="NULLABLE"),
            SchemaField("streak", "STRING", mode="NULLABLE"),      # 'W3' or 'L1'
            SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    }

    for table_name, schema in tables.items():
        table_ref = f"{dataset}.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"Table ready: {table_ref}")
