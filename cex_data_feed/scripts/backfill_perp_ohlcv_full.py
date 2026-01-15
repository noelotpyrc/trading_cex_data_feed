#!/usr/bin/env python3
"""
Backfill ohlcv_btcusdt_1h_full table from historical kline zip files.

This script reads the compressed CSV files from Binance Data Vision and inserts
all columns (including trade fields) into a new DuckDB table.

Usage:
    python -m cex_data_feed.scripts.backfill_perp_ohlcv_full --dry-run
    python -m cex_data_feed.scripts.backfill_perp_ohlcv_full
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


# Default paths
DEFAULT_DB_PATH = Path("/Volumes/Extreme SSD/trading_data/cex/db/binance_btcusdt_perp_ohlcv.duckdb")
DEFAULT_CSV_DIR = Path("/Volumes/Extreme SSD/trading_data/cex/ohlvc/binance_btcusdt_perp_1h")

TABLE_NAME = "ohlcv_btcusdt_1h_full"


def _connect(db_path: Path):
    """Create DuckDB connection, ensuring parent dir exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ensure_table(db_path: Path) -> None:
    """Create the ohlcv_btcusdt_1h_full table if not exists."""
    con = _connect(db_path)
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
              timestamp TIMESTAMP,
              snapshot_time TIMESTAMP,
              open DOUBLE,
              high DOUBLE,
              low DOUBLE,
              close DOUBLE,
              volume DOUBLE,
              quote_asset_volume DOUBLE,
              num_trades INTEGER,
              taker_buy_base_volume DOUBLE,
              taker_buy_quote_volume DOUBLE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_ts ON {TABLE_NAME}(timestamp);")
        print(f"[INFO] Ensured table {TABLE_NAME} exists")
    finally:
        con.close()


def read_zip_file(zip_path: Path) -> pd.DataFrame:
    """Read a single kline zip file and return DataFrame with proper columns."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Get the CSV file inside (usually same name as zip without .zip)
        csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
        if not csv_names:
            raise ValueError(f"No CSV found in {zip_path}")
        
        with zf.open(csv_names[0]) as f:
            # Check if file has header
            first_line = f.readline().decode('utf-8')
            f.seek(0)
            
            if first_line.startswith('open_time'):
                # Has header
                df = pd.read_csv(f)
            else:
                # No header - use column names
                df = pd.read_csv(f, header=None, names=[
                    'open_time', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'count', 'taker_buy_volume',
                    'taker_buy_quote_volume', 'ignore'
                ])
    
    # Convert timestamps
    df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
    df['snapshot_time'] = df['timestamp'] + pd.Timedelta(hours=1)
    
    # Prepare output DataFrame
    result = pd.DataFrame({
        'timestamp': df['timestamp'],
        'snapshot_time': df['snapshot_time'],
        'open': df['open'].astype(float),
        'high': df['high'].astype(float),
        'low': df['low'].astype(float),
        'close': df['close'].astype(float),
        'volume': df['volume'].astype(float),
        'quote_asset_volume': df['quote_volume'].astype(float),
        'num_trades': df['count'].astype(int),
        'taker_buy_base_volume': df['taker_buy_volume'].astype(float),
        'taker_buy_quote_volume': df['taker_buy_quote_volume'].astype(float),
    })
    
    return result


def backfill_from_zips(csv_dir: Path, db_path: Path, dry_run: bool = False) -> int:
    """Backfill ohlcv_btcusdt_1h_full from all zip files in directory."""
    
    # Find all zip files
    zip_files = sorted(csv_dir.glob("BTCUSDT-1h-*.zip"))
    if not zip_files:
        print(f"[ERROR] No zip files found in {csv_dir}")
        return 1
    
    print(f"Found {len(zip_files)} zip files")
    print(f"Range: {zip_files[0].name} to {zip_files[-1].name}")
    
    # Read all files into one DataFrame
    all_dfs = []
    for zf in zip_files:
        try:
            df = read_zip_file(zf)
            all_dfs.append(df)
            if len(all_dfs) % 12 == 0:  # Progress every year
                print(f"  Read {len(all_dfs)} files...")
        except Exception as e:
            print(f"[WARN] Failed to read {zf.name}: {e}")
    
    if not all_dfs:
        print("[ERROR] No data read from zip files")
        return 1
    
    combined = pd.concat(all_dfs, ignore_index=True)
    combined = combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
    
    print(f"Total rows: {len(combined)}")
    print(f"Date range: {combined['timestamp'].min()} to {combined['timestamp'].max()}")
    
    if dry_run:
        print("\n[DRY-RUN] Would insert:")
        print(combined.head())
        print("...")
        print(combined.tail())
        return 0
    
    # Ensure table exists
    ensure_table(db_path)
    
    # Insert data
    con = _connect(db_path)
    try:
        con.execute("SET TimeZone='UTC';")
        
        # Get existing timestamps to avoid duplicates
        existing = con.execute(f"SELECT timestamp FROM {TABLE_NAME}").fetchdf()
        existing_ts = set(existing['timestamp']) if not existing.empty else set()
        
        # Filter to new rows only
        insert_df = combined[~combined['timestamp'].isin(existing_ts)]
        print(f"New rows to insert: {len(insert_df)} (skipping {len(combined) - len(insert_df)} existing)")
        
        if len(insert_df) > 0:
            con.register("insert_data", insert_df)
            con.execute(f"""
                INSERT INTO {TABLE_NAME} (
                    timestamp, snapshot_time, open, high, low, close, volume,
                    quote_asset_volume, num_trades, taker_buy_base_volume, taker_buy_quote_volume
                )
                SELECT 
                    timestamp, snapshot_time, open, high, low, close, volume,
                    quote_asset_volume, num_trades, taker_buy_base_volume, taker_buy_quote_volume
                FROM insert_data
            """)
            print(f"[INFO] Inserted {len(insert_df)} rows into {TABLE_NAME}")
        else:
            print("[INFO] No new rows to insert")
        
        # Show final stats
        stats = con.execute(f"""
            SELECT COUNT(*) as cnt, MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
            FROM {TABLE_NAME}
        """).fetchdf()
        print(f"\nTable stats: {stats['cnt'].iloc[0]} rows, {stats['min_ts'].iloc[0]} to {stats['max_ts'].iloc[0]}")
        
    finally:
        con.close()
    
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill ohlcv_btcusdt_1h_full from historical kline zips",
    )
    p.add_argument("--db", type=Path, default=DEFAULT_DB_PATH,
                   help=f"Path to DuckDB file (default: {DEFAULT_DB_PATH})")
    p.add_argument("--csv-dir", type=Path, default=DEFAULT_CSV_DIR,
                   help=f"Directory with zip files (default: {DEFAULT_CSV_DIR})")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would be inserted without writing")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    
    print(f"=== Backfill {TABLE_NAME} ===")
    print(f"Source: {args.csv_dir}")
    print(f"Target: {args.db}")
    
    return backfill_from_zips(args.csv_dir, args.db, args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
