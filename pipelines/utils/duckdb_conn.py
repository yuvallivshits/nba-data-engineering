import os

import duckdb


def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """
    Open a DuckDB connection.

    db_path can be:
      - A file path like "/data/gold/nba.duckdb"  -> persistent database
      - ":memory:"                                 -> in-memory (used in tests + CI)
      - None                                       -> reads DUCKDB_PATH env var
    """
    path = db_path or os.environ.get("DUCKDB_PATH", ":memory:")
    return duckdb.connect(path)


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create all gold layer tables if they do not already exist.
    Safe to run multiple times (idempotent).

    Dates are stored as DATE type — format-neutral in the database.
    Display formatting (DD-MM-YYYY) is handled in the Streamlit dashboard.
    """

    # Calendar table — generated once for the full 2024-25 season.
    # Used to filter and group data by week, month, etc. in the dashboard.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS date_dim (
            date_key     INTEGER PRIMARY KEY,  -- YYYYMMDD e.g. 20241015
            full_date    DATE    NOT NULL,
            day_of_week  INTEGER NOT NULL,     -- 0=Monday, 6=Sunday
            day_name     VARCHAR NOT NULL,
            day_of_month INTEGER NOT NULL,
            month        INTEGER NOT NULL,
            month_name   VARCHAR NOT NULL,
            quarter      INTEGER NOT NULL,
            year         INTEGER NOT NULL,
            is_weekend   BOOLEAN NOT NULL,
            season       VARCHAR NOT NULL      -- '2024-25'
        )
    """)

    # Static team info — SCD Type 1 (simple upsert, no history).
    # Teams rarely change, and when they do (e.g. city rename) we just overwrite.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_dim (
            team_id      INTEGER PRIMARY KEY,
            full_name    VARCHAR NOT NULL,
            abbreviation VARCHAR NOT NULL,
            nickname     VARCHAR NOT NULL,
            city         VARCHAR NOT NULL,
            state        VARCHAR NOT NULL,
            conference   VARCHAR,
            division     VARCHAR,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    # Static player info only — name, active status.
    # No team_id here — team allocation is tracked separately in player_team_scd.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_dim (
            player_id    INTEGER PRIMARY KEY,
            full_name    VARCHAR NOT NULL,
            first_name   VARCHAR NOT NULL,
            last_name    VARCHAR NOT NULL,
            is_active    BOOLEAN NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    # Tracks which team a player plays for over time — SCD Type 2.
    # When a player is traded:
    #   - Close current row: valid_to = trade_date - 1, is_current = FALSE
    #   - Insert new row:    valid_from = trade_date, valid_to = '9999-12-31', is_current = TRUE
    #
    # To find a player's team on a specific game date, join on:
    #   player_id = ? AND valid_from <= game_date AND valid_to >= game_date
    conn.execute("CREATE SEQUENCE IF NOT EXISTS player_team_scd_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_team_scd (
            surrogate_key INTEGER PRIMARY KEY DEFAULT nextval('player_team_scd_seq'),
            player_id     INTEGER NOT NULL,
            team_id       INTEGER NOT NULL,
            valid_from    DATE    NOT NULL,
            valid_to      DATE    NOT NULL,  -- '9999-12-31' = currently active
            is_current    BOOLEAN NOT NULL,
            inserted_at   TIMESTAMP NOT NULL
        )
    """)

    # One row per game — stores raw scores only.
    # Winner is derived at query time: home_score > away_score → home team won.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS game_fact (
            game_id      VARCHAR   NOT NULL PRIMARY KEY,
            game_date    DATE      NOT NULL,
            home_team_id INTEGER   NOT NULL,
            away_team_id INTEGER   NOT NULL,
            home_score   INTEGER,
            away_score   INTEGER,
            season       VARCHAR   NOT NULL,
            season_type  VARCHAR   NOT NULL,  -- 'Regular Season', 'Playoffs'
            status       VARCHAR   NOT NULL,
            arena        VARCHAR,
            loaded_at    TIMESTAMP NOT NULL
        )
    """)

    # One row per player per game — all raw stats, no derived percentages.
    # FG%, 3P%, FT% are computed at query time: fg_made / fg_attempted etc.
    # To get the team a player was on during this game, join player_team_scd:
    #   player_id = ? AND valid_from <= game_date AND valid_to >= game_date
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_game_stats_fact (
            game_id             VARCHAR   NOT NULL,
            player_id           INTEGER   NOT NULL,
            game_date           DATE      NOT NULL,
            minutes             FLOAT,
            points              INTEGER,
            rebounds_offensive  INTEGER,
            rebounds_defensive  INTEGER,
            assists             INTEGER,
            steals              INTEGER,
            blocks              INTEGER,
            turnovers           INTEGER,
            fouls               INTEGER,
            sportsmanship_fouls INTEGER,
            is_ejected          BOOLEAN,
            fg_made             INTEGER,
            fg_attempted        INTEGER,
            fg3_made            INTEGER,
            fg3_attempted       INTEGER,
            ft_made             INTEGER,
            ft_attempted        INTEGER,
            plus_minus          INTEGER,
            loaded_at           TIMESTAMP NOT NULL,
            PRIMARY KEY (game_id, player_id)
        )
    """)

    # One row per team per day — captures full standings state each day.
    # Stored daily even on no-game days (values repeated) so the dashboard
    # can draw a continuous line chart without gaps.
    # win_pct is derived at query time: wins / (wins + losses)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS standings_snapshot (
            snapshot_date   DATE    NOT NULL,
            team_id         INTEGER NOT NULL,
            wins            INTEGER NOT NULL,
            losses          INTEGER NOT NULL,
            conference      VARCHAR NOT NULL,
            division        VARCHAR NOT NULL,
            conference_rank INTEGER,
            division_rank   INTEGER,
            home_wins       INTEGER,
            home_losses     INTEGER,
            away_wins       INTEGER,
            away_losses     INTEGER,
            last_10_wins    INTEGER,
            last_10_losses  INTEGER,
            streak          VARCHAR,            -- e.g. 'W3' or 'L1'
            loaded_at       TIMESTAMP NOT NULL,
            PRIMARY KEY (snapshot_date, team_id)
        )
    """)
