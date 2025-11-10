#!/usr/bin/env python3
from __future__ import annotations

"""
Export all OHLCV rows from a DuckDB table to a CSV under the repo's data/ folder.

Usage examples:
  python utils/export_duckdb_ohlcv_to_csv.py \
    --duckdb "/Volumes/Extreme SSD/trading_data/cex/db/binance_btcusdt_perp_ohlcv.duckdb" \
    --table ohlcv_btcusdt_1h \
    --out "data/BINANCE_BTCUSDT.P, 60.csv" --overwrite

Notes:
  - Outputs columns: timestamp, open, high, low, close, volume (UTC-naive timestamps)
  - By default prevents overwriting unless --overwrite is passed
"""

import argparse
from pathlib import Path
import sys

import pandas as pd


def _proj_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> None:
    root = _proj_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from run.data_loader import load_ohlcv_duckdb

    parser = argparse.ArgumentParser(description='Export OHLCV from DuckDB to CSV')
    parser.add_argument('--duckdb', required=True, help='Path to DuckDB file')
    parser.add_argument('--table', default='ohlcv_btcusdt_1h', help='DuckDB OHLCV table name')
    parser.add_argument('--out', default=str(root / 'data' / 'BINANCE_BTCUSDT.P, 60.csv'), help='Output CSV path')
    parser.add_argument('--overwrite', action='store_true', help='Allow overwriting existing output file')
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and not args.overwrite:
        print(f"ERROR: Output exists: {out_path}. Pass --overwrite to replace.")
        sys.exit(2)

    df = load_ohlcv_duckdb(args.duckdb, table=args.table)
    if df.empty:
        print("WARN: No rows fetched from DuckDB; writing empty CSV with header.")
    # Ensure column order
    cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    for c in cols:
        if c not in df.columns:
            raise RuntimeError(f"Missing expected column in OHLCV: {c}")
    df = df[cols].copy()

    df.to_csv(out_path, index=False)
    if not df.empty:
        print(f"Wrote {len(df):,} rows to {out_path}")
        print(f"Range: {df['timestamp'].iloc[0]} .. {df['timestamp'].iloc[-1]}")
    else:
        print(f"Wrote empty CSV to {out_path}")


if __name__ == '__main__':
    main()

