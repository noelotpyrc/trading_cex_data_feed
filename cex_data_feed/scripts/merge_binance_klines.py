#!/usr/bin/env python3
"""
Unzip and merge Binance monthly kline ZIPs into a single CSV.

Assumes files are named like: {SYMBOL}-{INTERVAL}-YYYY-MM.zip and each zip
contains a single CSV file with the same name. Works for USDT-M futures monthly
klines obtained from Binance Vision data.

Default input directory:
  /Volumes/Extreme SSD/trading_data/cex/ohlvc

Usage examples:
  python utils/merge_binance_klines.py \
    --in-dir "/Volumes/Extreme SSD/trading_data/cex/ohlvc" \
    --symbol BTCUSDT --interval 1m \
    --out "/Volumes/Extreme SSD/trading_data/cex/ohlvc/BTCUSDT-1m-merged.csv"

Notes:
- The Binance CSV files generally have no header. This script writes a standard
  header by default (can be disabled with --no-header).
- If a file contains a header line, it will be skipped to avoid duplicates.
"""

from __future__ import annotations

import argparse
import glob
import io
import os
import re
import sys
import zipfile
from typing import Iterable, List


DEFAULT_INPUT_DIR = "/Volumes/Extreme SSD/trading_data/cex/ohlvc"

# Standard Binance kline column names for CSV
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


def is_header_line(line: str) -> bool:
    # Heuristic: header contains alphabetic chars or underscores, not purely digits/comma
    # Also catch known first token 'open_time'
    s = line.strip().lower()
    if not s:
        return False
    if s.startswith("open_time,"):
        return True
    # If any alpha character exists before first newline, assume header
    return any(c.isalpha() for c in s.split(",", 1)[0])


def iter_zip_csv_lines(zip_path: str) -> Iterable[str]:
    # Each zip should contain one CSV with the same base name
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Prefer the first .csv entry
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            return
        name = names[0]
        with zf.open(name, "r") as f:
            # Use utf-8-sig to strip potential BOM
            wrapper = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
            for line in wrapper:
                # Normalize line endings and skip blank lines
                if not line:
                    continue
                if line.strip() == "":
                    continue
                yield line.rstrip("\n\r")


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge Binance monthly kline ZIPs into one CSV")
    p.add_argument("--in-dir", default=DEFAULT_INPUT_DIR, help="Directory with monthly ZIP files")
    p.add_argument("--symbol", default="BTCUSDT", help="Symbol, e.g., BTCUSDT")
    p.add_argument("--interval", default="1m", help="Interval, e.g., 1m")
    p.add_argument("--out", default=None, help="Output CSV path; default computed from symbol/interval")
    p.add_argument("--no-header", action="store_true", help="Do not write header to merged CSV")
    p.add_argument("--overwrite", action="store_true", help="Overwrite output if it exists")
    return p.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)

    pattern = os.path.join(args.in_dir, f"{args.symbol}-{args.interval}-*.zip")
    zip_paths = sorted(glob.glob(pattern))
    if not zip_paths:
        print(f"[ERROR] No ZIPs found matching: {pattern}")
        return 2

    out_path = (
        args.out
        if args.out
        else os.path.join(args.in_dir, f"{args.symbol}-{args.interval}-merged.csv")
    )

    if os.path.exists(out_path) and not args.overwrite:
        print(f"[ERROR] Output exists: {out_path}. Use --overwrite to replace.")
        return 2

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    total_rows = 0
    with open(out_path, "w", encoding="utf-8", newline="\n") as out_f:
        if not args.no_header:
            out_f.write(",".join(BINANCE_KLINE_HEADER) + "\n")

        for idx, zp in enumerate(zip_paths, start=1):
            base = os.path.basename(zp)
            print(f"[{idx}/{len(zip_paths)}] Merging {base}")
            it = iter_zip_csv_lines(zp)
            first_line_checked = False
            for line in it:
                if not first_line_checked:
                    first_line_checked = True
                    if is_header_line(line):
                        # Skip header from the source file
                        continue
                out_f.write(line + "\n")
                total_rows += 1

    print(f"[INFO] Merged {len(zip_paths)} files into {out_path} with {total_rows} data rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


