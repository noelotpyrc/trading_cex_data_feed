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
    """Create OHLCV table if not exists. Also runs migration to add snapshot_time if needed."""
    con = _connect(db_path)
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
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
        
        # Migration: Add snapshot_time column if it doesn't exist (for existing tables)
        try:
            con.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN snapshot_time TIMESTAMP;")
            # Backfill: snapshot_time = timestamp + 1 hour for existing rows
            con.execute(f"UPDATE {TABLE_NAME} SET snapshot_time = timestamp + INTERVAL '1 hour' WHERE snapshot_time IS NULL;")
        except duckdb.CatalogException:
            pass  # Column already exists
    finally:
        con.close()


def read_last_n_rows_ending_before(db_path: Path, n: int, end_exclusive: pd.Timestamp) -> pd.DataFrame:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        q = f"""
            SELECT timestamp, snapshot_time, open, high, low, close, volume
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
            INSERT INTO {TABLE_NAME} (timestamp, snapshot_time, open, high, low, close, volume)
            SELECT ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_NAME} WHERE timestamp = ?
            );
            """,
            [
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
                pd.to_datetime(row["snapshot_time"]).to_pydatetime(),
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


# -----------------------------------------------------------------------------
# Open Interest Table (Historical Statistics)
# -----------------------------------------------------------------------------

TABLE_OPEN_INTEREST = "open_interest_btcusdt_1h"


def ensure_table_open_interest(db_path: Path) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_OPEN_INTEREST} (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
              sum_open_interest DOUBLE,
              sum_open_interest_value DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_OPEN_INTEREST}_ts ON {TABLE_OPEN_INTEREST}(timestamp);")
    finally:
        con.close()


def append_open_interest_if_absent(db_path: Path, row: pd.Series) -> None:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute(
            f"""
            INSERT INTO {TABLE_OPEN_INTEREST} (timestamp, snapshot_time, sum_open_interest, sum_open_interest_value)
            SELECT ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_OPEN_INTEREST} WHERE timestamp = ?
            );
            """,
            [
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
                pd.to_datetime(row["snapshot_time"]).to_pydatetime(),
                float(row["sum_open_interest"]),
                float(row["sum_open_interest_value"]),
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
            ],
        )
    finally:
        con.close()


def read_last_n_open_interest(db_path: Path, n: int, end_exclusive: pd.Timestamp) -> pd.DataFrame:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        q = f"""
            SELECT timestamp, snapshot_time, sum_open_interest, sum_open_interest_value
            FROM {TABLE_OPEN_INTEREST}
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        df = con.execute(q, [end_exclusive.to_pydatetime(), n]).fetch_df()
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    finally:
        con.close()


# -----------------------------------------------------------------------------
# Long/Short Ratio Table
# -----------------------------------------------------------------------------

TABLE_LONG_SHORT_RATIO = "long_short_ratio_btcusdt_1h"


def ensure_table_long_short_ratio(db_path: Path) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_LONG_SHORT_RATIO} (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
              long_short_ratio DOUBLE,
              long_account DOUBLE,
              short_account DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_LONG_SHORT_RATIO}_ts ON {TABLE_LONG_SHORT_RATIO}(timestamp);")
    finally:
        con.close()


def append_long_short_ratio_if_absent(db_path: Path, row: pd.Series) -> None:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute(
            f"""
            INSERT INTO {TABLE_LONG_SHORT_RATIO} (timestamp, snapshot_time, long_short_ratio, long_account, short_account)
            SELECT ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_LONG_SHORT_RATIO} WHERE timestamp = ?
            );
            """,
            [
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
                pd.to_datetime(row["snapshot_time"]).to_pydatetime(),
                float(row["long_short_ratio"]),
                float(row["long_account"]),
                float(row["short_account"]),
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
            ],
        )
    finally:
        con.close()


def read_last_n_long_short_ratio(db_path: Path, n: int, end_exclusive: pd.Timestamp) -> pd.DataFrame:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        q = f"""
            SELECT timestamp, snapshot_time, long_short_ratio, long_account, short_account
            FROM {TABLE_LONG_SHORT_RATIO}
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        df = con.execute(q, [end_exclusive.to_pydatetime(), n]).fetch_df()
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    finally:
        con.close()


# -----------------------------------------------------------------------------
# Premium Index Klines Table
# -----------------------------------------------------------------------------

TABLE_PREMIUM_INDEX = "premium_index_btcusdt_1h"


def ensure_table_premium_index(db_path: Path) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_PREMIUM_INDEX} (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
              open DOUBLE,
              high DOUBLE,
              low DOUBLE,
              close DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_PREMIUM_INDEX}_ts ON {TABLE_PREMIUM_INDEX}(timestamp);")
    finally:
        con.close()


def append_premium_index_if_absent(db_path: Path, row: pd.Series) -> None:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute(
            f"""
            INSERT INTO {TABLE_PREMIUM_INDEX} (timestamp, snapshot_time, open, high, low, close)
            SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_PREMIUM_INDEX} WHERE timestamp = ?
            );
            """,
            [
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
                pd.to_datetime(row["snapshot_time"]).to_pydatetime(),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
            ],
        )
    finally:
        con.close()


def read_last_n_premium_index(db_path: Path, n: int, end_exclusive: pd.Timestamp) -> pd.DataFrame:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        q = f"""
            SELECT timestamp, snapshot_time, open, high, low, close
            FROM {TABLE_PREMIUM_INDEX}
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        df = con.execute(q, [end_exclusive.to_pydatetime(), n]).fetch_df()
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    finally:
        con.close()


# -----------------------------------------------------------------------------
# Spot Klines Table
# -----------------------------------------------------------------------------

# NOTE: Spot table uses same name as perp OHLCV for consistency
TABLE_SPOT_OHLCV = "ohlcv_btcusdt_1h"


def ensure_table_spot_ohlcv(db_path: Path) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_SPOT_OHLCV} (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
              open DOUBLE,
              high DOUBLE,
              low DOUBLE,
              close DOUBLE,
              volume DOUBLE,
              num_trades INTEGER,
              taker_buy_base_volume DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_SPOT_OHLCV}_ts ON {TABLE_SPOT_OHLCV}(timestamp);")
    finally:
        con.close()


def append_spot_ohlcv_if_absent(db_path: Path, row: pd.Series) -> None:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute(
            f"""
            INSERT INTO {TABLE_SPOT_OHLCV} (timestamp, snapshot_time, open, high, low, close, volume, num_trades, taker_buy_base_volume)
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_SPOT_OHLCV} WHERE timestamp = ?
            );
            """,
            [
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
                pd.to_datetime(row["snapshot_time"]).to_pydatetime(),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                int(row["num_trades"]),
                float(row["taker_buy_base_volume"]),
                pd.to_datetime(row["timestamp"]).to_pydatetime(),
            ],
        )
    finally:
        con.close()


def read_last_n_spot_ohlcv(db_path: Path, n: int, end_exclusive: pd.Timestamp) -> pd.DataFrame:
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        q = f"""
            SELECT timestamp, snapshot_time, open, high, low, close, volume, num_trades, taker_buy_base_volume
            FROM {TABLE_SPOT_OHLCV}
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        df = con.execute(q, [end_exclusive.to_pydatetime(), n]).fetch_df()
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    finally:
        con.close()

