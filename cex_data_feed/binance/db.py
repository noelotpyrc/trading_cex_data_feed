from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb  # type: ignore
import pandas as pd


TABLE_NAME = "ohlcv_btcusdt_1h"


@dataclass
class DBConfig:
    path: Path


def _connect(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ensure_table(db_path: Path) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
              timestamp TIMESTAMP,
              open DOUBLE,
              high DOUBLE,
              low DOUBLE,
              close DOUBLE,
              volume DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        # Lightweight uniqueness guard via index; DuckDB does not enforce PK by default
        con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_ts ON {TABLE_NAME}(timestamp);")
    finally:
        con.close()


def read_last_n_rows_ending_before(db_path: Path, n: int, end_exclusive: pd.Timestamp) -> pd.DataFrame:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        q = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {TABLE_NAME}
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        df = con.execute(q, [end_exclusive.to_pydatetime(), n]).fetch_df()
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    finally:
        con.close()


def append_row_if_absent(db_path: Path, row: pd.Series) -> None:
    """Append a single row if timestamp does not already exist."""
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute(
            f"""
            INSERT INTO {TABLE_NAME} (timestamp, open, high, low, close, volume)
            SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_NAME} WHERE timestamp = ?
            );
            """,
            [
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
            ],
        )
    finally:
        con.close()


def coverage_stats(db_path: Path) -> Optional[tuple[pd.Timestamp, pd.Timestamp, int]]:
    con = _connect(db_path)
    try:
        q = f"SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM {TABLE_NAME}"
        res = con.execute(q).fetchone()
        if res is None or res[0] is None:
            return None
        return pd.Timestamp(res[0]), pd.Timestamp(res[1]), int(res[2])
    finally:
        con.close()

