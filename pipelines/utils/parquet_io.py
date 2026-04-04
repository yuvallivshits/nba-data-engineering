import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(
    df: pd.DataFrame,
    path: str,
    partition_cols: list[str] | None = None,
) -> None:
    """
    Write a DataFrame to Parquet.

    Two modes:
    1. Partitioned — creates subdirectories per partition value:
         path/game_date=2024-10-15/part-0.parquet
         path/game_date=2024-10-16/part-0.parquet
       Used for bronze/silver fact tables (games, player stats, standings)
       so each pipeline run only rewrites its own date partition.

    2. Single file — writes one file:
         path/data.parquet
       Used for dimension tables (teams, players) which are small and fully rewritten.

    Always overwrites — deletes existing path before writing.
    This guarantees idempotency: re-running a pipeline for the same date
    produces the same result, never duplicates.
    """
    path_obj = Path(path)

    if partition_cols:
        if path_obj.exists():
            shutil.rmtree(path_obj)
        path_obj.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=str(path_obj),
            partition_cols=partition_cols,
            compression="snappy",
        )
    else:
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(path_obj), compression="snappy")


def read_parquet(
    path: str,
    filters: list[tuple[str, str, str]] | None = None,
) -> pd.DataFrame:
    """
    Read Parquet files into a DataFrame.

    Works for both single files and partitioned datasets.

    filters: optional list of filter tuples for partition pruning.
    Partition pruning means only the matching partition directories
    are read from disk — much faster than reading everything and filtering.

    Example (only read games for one date):
        read_parquet("data/bronze/games", filters=[("game_date", "=", "2024-10-15")])
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"Parquet path does not exist: {path}")

    table = pq.read_table(str(path_obj), filters=filters)
    return table.to_pandas()


def delete_partition(path: str) -> None:
    """
    Delete a single partition directory if it exists.

    Called before re-writing a partition to guarantee idempotency.

    Example:
        delete_partition("data/bronze/games/game_date=2024-10-15")
    """
    path_obj = Path(path)
    if path_obj.exists():
        shutil.rmtree(path_obj)
