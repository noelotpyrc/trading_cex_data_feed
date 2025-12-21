#!/usr/bin/env python3
"""
Generate clean hourly metrics CSV from raw 5-minute metrics data.

Gap-filling logic for OI and Long/Short Ratio:
1. Primary: Use the minute=0 row as the snapshot for that hour
2. Fallback: If minute=0 is missing, use the most recent row from the PREVIOUS hour
   (since that's the closest data point before the snapshot time)
3. Missing: If the entire previous hour has no data, insert NaN

Output: Clean 1h CSV with columns:
  - snapshot_time: The hour boundary (e.g., 04:00:00)
  - timestamp: snapshot_time - 1h (aligned with OHLCV candle)
  - is_fallback: Boolean indicating if fallback logic was used
  - OI and L/S ratio columns

Usage:
    python -m cex_data_feed.scripts.clean_metrics_1h
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "data"
INPUT_CSV = "BTCUSDT-metrics-2021-12-2025-12.csv"
OUTPUT_CSV = "BTCUSDT-metrics-1h-clean.csv"


def clean_metrics_to_hourly(input_path: Path, output_path: Path, dry_run: bool = False) -> int:
    """Generate clean hourly metrics from 5-minute data with gap filling."""
    
    print(f"=== Cleaning Metrics to Hourly ===")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    
    # Load raw data
    df = pd.read_csv(input_path)
    df["ts"] = pd.to_datetime(df["create_time"])
    df = df.sort_values("ts").reset_index(drop=True)
    
    print(f"Raw rows: {len(df)}")
    print(f"Date range: {df['ts'].min()} to {df['ts'].max()}")
    
    # Determine full hourly range (using the :00 boundaries)
    ts_min = df["ts"].min().floor("h")
    ts_max = df["ts"].max().ceil("h")
    
    # Generate all expected hourly timestamps (these are snapshot_times)
    all_hours = pd.date_range(ts_min, ts_max, freq="h")
    print(f"Expected hourly timestamps: {len(all_hours)}")
    
    # Columns to extract
    value_cols = [
        "sum_open_interest",
        "sum_open_interest_value",
        "count_long_short_ratio",
        "count_toptrader_long_short_ratio", 
        "sum_toptrader_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]
    
    # Build hourly data with gap filling
    records = []
    fallback_count = 0
    missing_count = 0
    
    for snapshot_time in all_hours:
        record = {
            "snapshot_time": snapshot_time,
            "timestamp": snapshot_time - pd.Timedelta(hours=1),
            "is_fallback": False,
        }
        
        # Primary: Look for exact minute=0 row at snapshot_time
        exact_match = df[df["ts"] == snapshot_time]
        
        if not exact_match.empty:
            # Use the exact :00 row
            row = exact_match.iloc[0]
            for col in value_cols:
                record[col] = row[col] if col in row.index else None
        else:
            # Fallback: Use the most recent row from the PREVIOUS hour
            # Previous hour = [snapshot_time - 1h, snapshot_time)
            prev_hour_start = snapshot_time - pd.Timedelta(hours=1)
            prev_hour_data = df[(df["ts"] >= prev_hour_start) & (df["ts"] < snapshot_time)]
            
            if not prev_hour_data.empty:
                # Take the most recent (last) row from previous hour
                row = prev_hour_data.iloc[-1]
                for col in value_cols:
                    record[col] = row[col] if col in row.index else None
                record["is_fallback"] = True
                fallback_count += 1
            else:
                # No data in previous hour - insert NaN
                for col in value_cols:
                    record[col] = None
                record["is_fallback"] = True
                missing_count += 1
        
        records.append(record)
    
    # Create output DataFrame
    result_df = pd.DataFrame(records)
    
    print(f"\n=== Summary ===")
    print(f"Total hourly rows: {len(result_df)}")
    print(f"Exact matches (minute=0): {len(result_df) - fallback_count - missing_count}")
    print(f"Fallback rows (used previous hour): {fallback_count - missing_count}")
    print(f"Missing rows (NaN): {missing_count}")
    
    if dry_run:
        print("\n[DRY-RUN] Sample output:")
        print(result_df.head(10).to_string())
        print("\n[DRY-RUN] Fallback examples:")
        print(result_df[result_df["is_fallback"]].head(5).to_string())
        return 0
    
    # Save to CSV
    result_df.to_csv(output_path, index=False)
    print(f"\n[INFO] Saved to {output_path}")
    
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean metrics CSV to hourly with gap filling")
    p.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    p.add_argument("--input", type=str, default=INPUT_CSV, help="Input CSV filename")
    p.add_argument("--output", type=str, default=OUTPUT_CSV, help="Output CSV filename")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    input_path = args.data_dir / args.input
    output_path = args.data_dir / args.output
    return clean_metrics_to_hourly(input_path, output_path, args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
