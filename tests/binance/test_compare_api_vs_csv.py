#!/usr/bin/env python3
from __future__ import annotations

"""
Compare OHLCV between the local CSV (randomly sampled timestamps) and the
DuckDB store. Prints values side-by-side for visual inspection.

Usage:
  python feed_binance_btcusdt_perp/tests/test_compare_api_vs_csv.py \
    --csv-path data/BINANCE_BTCUSDT.P, 60.csv \
    --duckdb "/Volumes/Extreme SSD/trading_data/cex/db/binance_btcusdt_perp_ohlcv.duckdb" \
    --table ohlcv_btcusdt_1h \
    --count 2 --seed 0

Notes:
  - No network calls. Compares local CSV rows against DuckDB rows by timestamp.
  - It does not assert equality; it only prints both sources' OHLCV values and
    simple diffs to aid manual comparison.
"""

import argparse
from pathlib import Path
import sys
import duckdb  # type: ignore

import numpy as np
import pandas as pd


def _proj_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_csv_ohlcv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    df = pd.read_csv(path)
    # Normalize timestamp to UTC-naive
    if 'timestamp' not in df.columns:
        first = df.columns[0]
        df = df.rename(columns={first: 'timestamp'})
    ts = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    df['timestamp'] = ts.dt.tz_convert('UTC').dt.tz_localize(None)
    # Standardize column names
    df.columns = [str(c).strip().lower() for c in df.columns]
    # Keep only needed columns
    need = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")
    df = df[need].copy()
    # Deduplicate and sort
    df = df.dropna(subset=['timestamp'])
    df = df[~df['timestamp'].duplicated(keep='last')]
    df = df.sort_values('timestamp').reset_index(drop=True)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description='Compare DuckDB vs CSV OHLCV for randomly sampled timestamps from CSV')
    parser.add_argument('--csv-path', default=None, help='Path to local 1H OHLCV CSV (defaults to repo data file)')
    parser.add_argument('--duckdb', default='/Volumes/Extreme SSD/trading_data/cex/db/binance_btcusdt_perp_ohlcv.duckdb', help='Path to DuckDB file containing the OHLCV table')
    parser.add_argument('--table', default='ohlcv_btcusdt_1h', help='DuckDB table name for OHLCV')
    parser.add_argument('--count', type=int, default=2, help='How many timestamps to sample from CSV (default: 2)')
    parser.add_argument('--seed', type=int, default=0, help='Random seed for reproducible sampling')
    args = parser.parse_args()

    root = _proj_root()
    if str(root) not in sys.path:
        sys.path.append(str(root))

    # No network/API used

    # Load CSV
    csv_path = Path(args.csv_path) if args.csv_path else (root / 'data' / 'BINANCE_BTCUSDT.P, 60.csv')
    csv_df = _read_csv_ohlcv(csv_path)

    # Sample timestamps from CSV
    rng = np.random.default_rng(int(args.seed))
    n = int(args.count)
    if n <= 0:
        print('[ERROR] --count must be >= 1')
        return
    if len(csv_df) == 0:
        print('[ERROR] CSV has no rows')
        return
    idxs = rng.choice(len(csv_df), size=min(n, len(csv_df)), replace=False)
    sampled = csv_df.iloc[sorted(idxs.tolist())].copy()

    db_path = Path(args.duckdb)
    if not db_path.exists():
        print(f"SKIP: DuckDB path not found: {db_path}")
        return

    print(f"CSV path: {csv_path}")
    print(f"DuckDB: {db_path}  Table: {args.table}")
    print(f"Sampling {len(sampled)} timestamp(s) with seed={args.seed}")

    con = duckdb.connect(str(db_path))
    try:
        con.execute("SET TimeZone='UTC';")
        for _, row in sampled.iterrows():
            ts = pd.Timestamp(row['timestamp'])
            q = f"SELECT timestamp, open, high, low, close, volume FROM {args.table} WHERE timestamp = ?"
            try:
                df_db = con.execute(q, [ts.to_pydatetime()]).fetch_df()
            except Exception as e:
                print(f"{ts}: DB query error: {e}")
                continue
            if df_db.empty:
                print(f"==== {ts} ====")
                print("DB : NOT FOUND")
                print(
                    "CSV:",
                    f"open={row['open']:.8f}",
                    f"high={row['high']:.8f}",
                    f"low={row['low']:.8f}",
                    f"close={row['close']:.8f}",
                    f"volume={row['volume']:.6f}",
                )
                continue
            db_row = df_db.iloc[0][['open','high','low','close','volume']].astype(float)
            csv_row = row[['open','high','low','close','volume']].astype(float)
            diffs = (db_row - csv_row).abs()
            print(f"==== {ts} ====")
            print(
                "DB :",
                f"open={db_row['open']:.8f}",
                f"high={db_row['high']:.8f}",
                f"low={db_row['low']:.8f}",
                f"close={db_row['close']:.8f}",
                f"volume={db_row['volume']:.6f}",
            )
            print(
                "CSV:",
                f"open={csv_row['open']:.8f}",
                f"high={csv_row['high']:.8f}",
                f"low={csv_row['low']:.8f}",
                f"close={csv_row['close']:.8f}",
                f"volume={csv_row['volume']:.6f}",
            )
            print(
                "DIFF:",
                f"open={diffs['open']:.10f}",
                f"high={diffs['high']:.10f}",
                f"low={diffs['low']:.10f}",
                f"close={diffs['close']:.10f}",
                f"volume={diffs['volume']:.10f}",
            )
    finally:
        con.close()

    print('compare_api_vs_csv OK')


if __name__ == '__main__':
    main()
