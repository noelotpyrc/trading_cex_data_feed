#!/usr/bin/env python3
"""
Backfill historical data from CSV files into DuckDB databases.

This script reads the merged CSV files and inserts data into the new DuckDB databases
with proper timestamp alignment (snapshot_time column).

Uses bulk inserts for performance (~10-100x faster than row-by-row).

Usage:
    python -m cex_data_feed.scripts.backfill_from_csv --all
    python -m cex_data_feed.scripts.backfill_from_csv --type open_interest
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import yaml

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

# Default paths
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"
DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "data"

# CSV file names (updated naming with 2025-12)
CSV_FILES = {
    "metrics": "BTCUSDT-metrics-2021-12-2025-12.csv",
    "premium_index": "BTCUSDT-1h-premium-index-2020-01-2025-12.csv",
    "spot": "BTCUSDT-1h-spot-2020-01-2025-12.csv",
}


def load_config(config_path: Path) -> dict:
    """Load configuration from YAML file."""
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def _connect(db_path: Path):
    """Create DuckDB connection, ensuring parent dir exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def backfill_open_interest(data_dir: Path, db_path: Path, dry_run: bool = False) -> int:
    """Backfill Open Interest from metrics CSV using bulk insert."""
    csv_path = data_dir / CSV_FILES["metrics"]
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        return 1
    
    print(f"\n=== Backfilling Open Interest ===")
    print(f"Source: {csv_path}")
    print(f"Target: {db_path}")
    
    # Read and prepare data
    df = pd.read_csv(csv_path)
    df["ts"] = pd.to_datetime(df["create_time"])
    df = df[df["ts"].dt.minute == 0].copy()  # Hourly only
    
    # Transform timestamps
    df["snapshot_time"] = df["ts"]
    df["timestamp"] = df["snapshot_time"] - pd.Timedelta(hours=1)
    
    # Select and rename columns for DB
    insert_df = df[["timestamp", "snapshot_time", "sum_open_interest", "sum_open_interest_value"]].copy()
    insert_df.columns = ["timestamp", "snapshot_time", "sum_open_interest", "sum_open_interest_value"]
    
    print(f"Rows to insert: {len(insert_df)}")
    
    if dry_run:
        print("[DRY-RUN] Would insert:")
        print(insert_df.head())
        return 0
    
    # Bulk insert using DuckDB
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        
        # Create table if not exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS open_interest_btcusdt_1h (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
              sum_open_interest DOUBLE,
              sum_open_interest_value DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_open_interest_btcusdt_1h_ts ON open_interest_btcusdt_1h(timestamp);")
        
        # Get existing timestamps to avoid duplicates
        existing = con.execute("SELECT timestamp FROM open_interest_btcusdt_1h").fetchdf()
        existing_ts = set(existing["timestamp"]) if not existing.empty else set()
        
        # Filter to new rows only
        insert_df = insert_df[~insert_df["timestamp"].isin(existing_ts)]
        print(f"New rows after filtering: {len(insert_df)}")
        
        if len(insert_df) > 0:
            # Register DataFrame and bulk insert
            con.register("insert_data", insert_df)
            con.execute("""
                INSERT INTO open_interest_btcusdt_1h (timestamp, snapshot_time, sum_open_interest, sum_open_interest_value)
                SELECT timestamp, snapshot_time, sum_open_interest, sum_open_interest_value FROM insert_data
            """)
            print(f"[INFO] Inserted {len(insert_df)} rows")
        else:
            print("[INFO] No new rows to insert")
    finally:
        con.close()
    
    return 0


def backfill_long_short_ratio(data_dir: Path, db_path: Path, dry_run: bool = False) -> int:
    """Backfill Long/Short Ratio from metrics CSV using bulk insert."""
    csv_path = data_dir / CSV_FILES["metrics"]
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        return 1
    
    print(f"\n=== Backfilling Long/Short Ratio ===")
    print(f"Source: {csv_path}")
    print(f"Target: {db_path}")
    
    # Read and prepare data
    df = pd.read_csv(csv_path)
    df["ts"] = pd.to_datetime(df["create_time"])
    df = df[df["ts"].dt.minute == 0].copy()
    
    # Transform timestamps
    df["snapshot_time"] = df["ts"]
    df["timestamp"] = df["snapshot_time"] - pd.Timedelta(hours=1)
    
    # Prepare columns, filling NaN with 0
    insert_df = pd.DataFrame({
        "timestamp": df["timestamp"],
        "snapshot_time": df["snapshot_time"],
        "long_short_ratio": df["count_long_short_ratio"].fillna(0),
        "long_account": df["count_toptrader_long_short_ratio"].fillna(0),
        "short_account": df["sum_toptrader_long_short_ratio"].fillna(0),
    })
    
    print(f"Rows to insert: {len(insert_df)}")
    
    if dry_run:
        print("[DRY-RUN] Would insert:")
        print(insert_df.head())
        return 0
    
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS long_short_ratio_btcusdt_1h (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
              long_short_ratio DOUBLE,
              long_account DOUBLE,
              short_account DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_long_short_ratio_btcusdt_1h_ts ON long_short_ratio_btcusdt_1h(timestamp);")
        
        existing = con.execute("SELECT timestamp FROM long_short_ratio_btcusdt_1h").fetchdf()
        existing_ts = set(existing["timestamp"]) if not existing.empty else set()
        insert_df = insert_df[~insert_df["timestamp"].isin(existing_ts)]
        print(f"New rows after filtering: {len(insert_df)}")
        
        if len(insert_df) > 0:
            con.register("insert_data", insert_df)
            con.execute("""
                INSERT INTO long_short_ratio_btcusdt_1h (timestamp, snapshot_time, long_short_ratio, long_account, short_account)
                SELECT timestamp, snapshot_time, long_short_ratio, long_account, short_account FROM insert_data
            """)
            print(f"[INFO] Inserted {len(insert_df)} rows")
        else:
            print("[INFO] No new rows to insert")
    finally:
        con.close()
    
    return 0


def backfill_premium_index(data_dir: Path, db_path: Path, dry_run: bool = False) -> int:
    """Backfill Premium Index from CSV using bulk insert."""
    csv_path = data_dir / CSV_FILES["premium_index"]
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        return 1
    
    print(f"\n=== Backfilling Premium Index ===")
    print(f"Source: {csv_path}")
    print(f"Target: {db_path}")
    
    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
    df["snapshot_time"] = df["timestamp"] + pd.Timedelta(hours=1)
    
    insert_df = pd.DataFrame({
        "timestamp": df["timestamp"],
        "snapshot_time": df["snapshot_time"],
        "open": df["open"],
        "high": df["high"],
        "low": df["low"],
        "close": df["close"],
    })
    
    print(f"Rows to insert: {len(insert_df)}")
    
    if dry_run:
        print("[DRY-RUN] Would insert:")
        print(insert_df.head())
        return 0
    
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS premium_index_btcusdt_1h (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
              open DOUBLE,
              high DOUBLE,
              low DOUBLE,
              close DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_premium_index_btcusdt_1h_ts ON premium_index_btcusdt_1h(timestamp);")
        
        existing = con.execute("SELECT timestamp FROM premium_index_btcusdt_1h").fetchdf()
        existing_ts = set(existing["timestamp"]) if not existing.empty else set()
        insert_df = insert_df[~insert_df["timestamp"].isin(existing_ts)]
        print(f"New rows after filtering: {len(insert_df)}")
        
        if len(insert_df) > 0:
            con.register("insert_data", insert_df)
            con.execute("""
                INSERT INTO premium_index_btcusdt_1h (timestamp, snapshot_time, open, high, low, close)
                SELECT timestamp, snapshot_time, open, high, low, close FROM insert_data
            """)
            print(f"[INFO] Inserted {len(insert_df)} rows")
        else:
            print("[INFO] No new rows to insert")
    finally:
        con.close()
    
    return 0


def backfill_spot_ohlcv(data_dir: Path, db_path: Path, dry_run: bool = False) -> int:
    """Backfill Spot OHLCV from CSV using bulk insert."""
    csv_path = data_dir / CSV_FILES["spot"]
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        return 1
    
    print(f"\n=== Backfilling Spot OHLCV ===")
    print(f"Source: {csv_path}")
    print(f"Target: {db_path}")
    
    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
    df["snapshot_time"] = df["timestamp"] + pd.Timedelta(hours=1)
    
    insert_df = pd.DataFrame({
        "timestamp": df["timestamp"],
        "snapshot_time": df["snapshot_time"],
        "open": df["open"],
        "high": df["high"],
        "low": df["low"],
        "close": df["close"],
        "volume": df["volume"],
        "num_trades": df["number_of_trades"],
        "taker_buy_base_volume": df["taker_buy_base_asset_volume"],
    })
    
    print(f"Rows to insert: {len(insert_df)}")
    
    if dry_run:
        print("[DRY-RUN] Would insert:")
        print(insert_df.head())
        return 0
    
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_btcusdt_1h (
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
        """)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_btcusdt_1h_ts ON ohlcv_btcusdt_1h(timestamp);")
        
        existing = con.execute("SELECT timestamp FROM ohlcv_btcusdt_1h").fetchdf()
        existing_ts = set(existing["timestamp"]) if not existing.empty else set()
        insert_df = insert_df[~insert_df["timestamp"].isin(existing_ts)]
        print(f"New rows after filtering: {len(insert_df)}")
        
        if len(insert_df) > 0:
            con.register("insert_data", insert_df)
            con.execute("""
                INSERT INTO ohlcv_btcusdt_1h (timestamp, snapshot_time, open, high, low, close, volume, num_trades, taker_buy_base_volume)
                SELECT timestamp, snapshot_time, open, high, low, close, volume, num_trades, taker_buy_base_volume FROM insert_data
            """)
            print(f"[INFO] Inserted {len(insert_df)} rows")
        else:
            print("[INFO] No new rows to insert")
    finally:
        con.close()
    
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill historical CSV data into DuckDB (bulk insert)",
    )
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    p.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    p.add_argument("--type", choices=["open_interest", "long_short_ratio", "premium_index", "spot_ohlcv"])
    p.add_argument("--all", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    
    if not args.type and not args.all:
        print("[ERROR] Specify --type or --all")
        return 1
    
    config = load_config(args.config)
    db_paths = config.get("db_paths", {})
    
    results = []
    
    if args.all or args.type == "open_interest":
        db_path = Path(db_paths.get("open_interest", "/tmp/binance_btcusdt_perp_open_interest.duckdb"))
        rc = backfill_open_interest(args.data_dir, db_path, args.dry_run)
        results.append(("open_interest", rc))
    
    if args.all or args.type == "long_short_ratio":
        db_path = Path(db_paths.get("long_short_ratio", "/tmp/binance_btcusdt_perp_long_short_ratio.duckdb"))
        rc = backfill_long_short_ratio(args.data_dir, db_path, args.dry_run)
        results.append(("long_short_ratio", rc))
    
    if args.all or args.type == "premium_index":
        db_path = Path(db_paths.get("premium_index", "/tmp/binance_btcusdt_perp_premium_index.duckdb"))
        rc = backfill_premium_index(args.data_dir, db_path, args.dry_run)
        results.append(("premium_index", rc))
    
    if args.all or args.type == "spot_ohlcv":
        db_path = Path(db_paths.get("spot_ohlcv", "/tmp/binance_btcusdt_spot_ohlcv.duckdb"))
        rc = backfill_spot_ohlcv(args.data_dir, db_path, args.dry_run)
        results.append(("spot_ohlcv", rc))
    
    print("\n=== Summary ===")
    for name, rc in results:
        status = "✓ OK" if rc == 0 else "✗ FAILED"
        print(f"  {name}: {status}")
    
    return 0 if all(rc == 0 for _, rc in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
