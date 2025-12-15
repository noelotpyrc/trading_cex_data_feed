#!/usr/bin/env python3
"""
Merge Binance data ZIPs (metrics, premium index, funding rate) into single CSVs.

Each ZIP is expected to contain a single CSV with a header.
The script merges them, keeping the header from the first file and skipping it for others.

Supported types:
  - metrics: Daily metrics (e.g., Open Interest)
  - premium_index: Premium Index Klines
  - funding_rate: Monthly funding rate history

Usage:
  python scripts/merge_binance_data.py \
    --type metrics \
    --symbol BTCUSDT \
    --in-dir ./data/metrics \
    --out ./data/BTCUSDT-metrics-merged.csv
"""

from __future__ import annotations

import argparse
import glob
import io
import os
import sys
import zipfile
from typing import Iterable, List, Optional


# Standard Binance kline column names for CSV (used when source has no header)
BINANCE_KLINE_HEADER = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]


def iter_zip_csv_lines(zip_path: str) -> Iterable[str]:
    """Yield lines from the first CSV found in the ZIP."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            return
        # Usually there's only one CSV
        name = names[0]
        with zf.open(name, "r") as f:
            # Use utf-8-sig to strip potential BOM
            wrapper = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
            for line in wrapper:
                if line:
                    yield line.rstrip("\n\r")


def get_file_pattern(data_type: str, symbol: str, interval: Optional[str]) -> str:
    if data_type == "metrics":
        # Pattern: {SYMBOL}-metrics-YYYY-MM-DD.zip
        return f"{symbol}-metrics-*.zip"
    elif data_type == "premium_index":
        # Pattern: {SYMBOL}-{INTERVAL}-YYYY-MM-DD.zip
        if not interval:
            raise ValueError("Interval is required for premium_index type")
        return f"{symbol}-{interval}-*.zip"
    elif data_type == "funding_rate":
        # Pattern: {SYMBOL}-fundingRate-YYYY-MM.zip
        return f"{symbol}-fundingRate-*.zip"
    else:
        raise ValueError(f"Unknown data type: {data_type}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge Binance data ZIPs into a single CSV.")
    parser.add_argument(
        "--type",
        required=True,
        choices=["metrics", "premium_index", "funding_rate"],
        help="Type of data to merge",
    )
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol, e.g., BTCUSDT")
    parser.add_argument("--interval", help="Interval (required for premium_index), e.g., 1h")
    parser.add_argument("--in-dir", required=True, help="Input directory containing ZIP files")
    parser.add_argument("--out", required=True, help="Output CSV file path")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output if it exists")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    try:
        filename_pattern = get_file_pattern(args.type, args.symbol, args.interval)
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 1

    search_pattern = os.path.join(args.in_dir, filename_pattern)
    zip_paths = sorted(glob.glob(search_pattern))

    if not zip_paths:
        print(f"[ERROR] No ZIPs found matching: {search_pattern}")
        return 2

    if os.path.exists(args.out) and not args.overwrite:
        print(f"[ERROR] Output exists: {args.out}. Use --overwrite to replace.")
        return 2

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    print(f"[INFO] Found {len(zip_paths)} files to merge.")
    print(f"[INFO] Writing to {args.out} ...")

    total_rows = 0
    header_written = False

    # Special handling for premium_index which is known to lack headers in raw files
    is_premium_index = (args.type == "premium_index")

    with open(args.out, "w", encoding="utf-8", newline="\n") as out_f:
        # If it's premium index, we do NOT force write header upfront anymore.
        # We handle it per-file.

        for idx, zp in enumerate(zip_paths, start=1):
            base = os.path.basename(zp)
            # Simple progress indicator
            if idx % 10 == 0 or idx == len(zip_paths):
                print(f"  processing {idx}/{len(zip_paths)}: {base}...", end="\r")
            
            it = iter_zip_csv_lines(zp)
            try:
                first_line = next(it)
            except StopIteration:
                # Empty file
                continue

            # Handle header
            if not header_written:
                if is_premium_index:
                    if first_line.strip().startswith("open_time"):
                         # File has header, use it
                         out_f.write(first_line + "\n")
                    else:
                         # Missing header in file, write our standard one then the data line
                         out_f.write(",".join(BINANCE_KLINE_HEADER) + "\n")
                         out_f.write(first_line + "\n")
                else:
                     # Other types (metrics, funding) always have headers
                     out_f.write(first_line + "\n")
                header_written = True
            else:
                # We already wrote a header. Check if we need to skip the current file's header.
                if is_premium_index:
                     if first_line.strip().startswith("open_time"):
                         # Skip this line, it's a duplicate header
                         pass
                     else:
                         # It's data, write it
                         out_f.write(first_line + "\n")
                else:
                     # For metrics/funding, first line is always header, so skip it.
                     pass

            # Write the rest
            for line in it:
                out_f.write(line + "\n")
                total_rows += 1
    
    print() # Newline after progress
    print(f"[INFO] Done. Merged {len(zip_paths)} files. Total data rows: {total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

