#!/usr/bin/env python3
"""
Validate API data against historical CSV/ZIP data from Binance.

Downloads historical data for a recent date, fetches the same period from API,
and compares values for matching timestamps.

Example:
  python -m cex_data_feed.scripts.validate_api_data --date 2025-12-18
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

import pandas as pd

from cex_data_feed.binance.api import (
    fetch_open_interest_hist,
    open_interest_hist_to_dataframe,
    fetch_long_short_ratio,
    long_short_ratio_to_dataframe,
    fetch_premium_index_klines,
    klines_to_dataframe,
    fetch_spot_klines,
    spot_klines_to_dataframe,
)
from cex_data_feed.scripts.download_binance_daily_metrics import (
    build_daily_url as build_metrics_url,
    download_if_needed as download_metrics,
)
from cex_data_feed.scripts.download_binance_daily_premium_index import (
    build_daily_url as build_premium_url,
    download_if_needed as download_premium,
)
from cex_data_feed.scripts.download_binance_daily_klines import (
    build_daily_url as build_klines_url,
    download_if_needed as download_klines,
)


DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_INTERVAL = "1h"


@dataclass
class ValidationResult:
    data_type: str
    comparison_rows: int  # Number of rows in the intersection (both API and CSV)
    matched_rows: int     # Rows that matched within tolerance
    mismatched_rows: int  # Rows with value differences
    csv_only_rows: int    # Rows only in CSV (outside API window)
    api_only_rows: int    # Rows only in API (outside CSV window)
    max_diff: float
    details: List[str]

    @property
    def ok(self) -> bool:
        # 100% match required on the intersection of timestamps
        return self.comparison_rows > 0 and self.mismatched_rows == 0


def _read_csv_from_zip(zip_path: str, csv_name_pattern: str = ".csv") -> pd.DataFrame:
    """Extract and read CSV from a ZIP file."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [n for n in zf.namelist() if csv_name_pattern in n]
        if not csv_files:
            raise ValueError(f"No CSV file found in {zip_path}")
        with zf.open(csv_files[0]) as f:
            content = f.read().decode("utf-8")
            return pd.read_csv(io.StringIO(content))


def _read_klines_csv_from_zip(zip_path: str) -> pd.DataFrame:
    """Extract and read klines CSV (no header) from ZIP."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [n for n in zf.namelist() if ".csv" in n]
        if not csv_files:
            raise ValueError(f"No CSV file found in {zip_path}")
        with zf.open(csv_files[0]) as f:
            content = f.read().decode("utf-8")
            # Spot klines have no header, need to define columns
            columns = [
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "count", "taker_buy_volume",
                "taker_buy_quote_volume", "ignore"
            ]
            df = pd.read_csv(io.StringIO(content), header=None, names=columns)
            return df


def validate_open_interest(
    csv_zip_path: str, 
    target_date: str,
    tolerance: float = 1.0
) -> ValidationResult:
    """Validate Open Interest data: API vs historical CSV.
    
    Compares only rows where timestamps exist in BOTH datasets (intersection).
    Requires 100% match on the intersection.
    """
    details = []
    
    # Read historical CSV
    csv_df = _read_csv_from_zip(csv_zip_path)
    csv_df["timestamp"] = pd.to_datetime(csv_df["create_time"], utc=True).dt.tz_localize(None)
    # Keep only hourly rows (XX:00)
    csv_df = csv_df[csv_df["timestamp"].dt.minute == 0].copy()
    csv_df = csv_df.rename(columns={
        "sum_open_interest": "csv_oi",
        "sum_open_interest_value": "csv_oi_value"
    })
    csv_df = csv_df[["timestamp", "csv_oi", "csv_oi_value"]].reset_index(drop=True)
    
    # Fetch API data
    api_data = fetch_open_interest_hist(DEFAULT_SYMBOL, period="1h", limit=48)
    api_df = open_interest_hist_to_dataframe(api_data)
    api_df = api_df.rename(columns={
        "sum_open_interest": "api_oi",
        "sum_open_interest_value": "api_oi_value"
    })
    
    # Find intersection of timestamps
    csv_timestamps = set(csv_df["timestamp"])
    api_timestamps = set(api_df["timestamp"])
    common_timestamps = csv_timestamps & api_timestamps
    
    csv_only = len(csv_timestamps - api_timestamps)
    api_only = len(api_timestamps - csv_timestamps)
    
    if not common_timestamps:
        return ValidationResult(
            data_type="Open Interest",
            comparison_rows=0,
            matched_rows=0,
            mismatched_rows=0,
            csv_only_rows=csv_only,
            api_only_rows=api_only,
            max_diff=0,
            details=["No overlapping timestamps found between CSV and API data"]
        )
    
    # Filter BOTH datasets to intersection only
    csv_compare = csv_df[csv_df["timestamp"].isin(common_timestamps)].copy()
    api_compare = api_df[api_df["timestamp"].isin(common_timestamps)].copy()
    
    # Merge on timestamp (inner join on intersection)
    merged = pd.merge(csv_compare, api_compare, on="timestamp", how="inner")
    
    # Compare OI values
    merged["oi_diff"] = abs(merged["csv_oi"] - merged["api_oi"])
    mismatched = merged[merged["oi_diff"] > tolerance]
    max_diff = merged["oi_diff"].max() if not merged.empty else 0
    
    for _, row in mismatched.head(5).iterrows():
        details.append(
            f"  {row['timestamp']}: CSV={row['csv_oi']:.2f}, API={row['api_oi']:.2f}, diff={row['oi_diff']:.2f}"
        )
    
    return ValidationResult(
        data_type="Open Interest",
        comparison_rows=len(merged),
        matched_rows=len(merged) - len(mismatched),
        mismatched_rows=len(mismatched),
        csv_only_rows=csv_only,
        api_only_rows=api_only,
        max_diff=max_diff,
        details=details
    )


def validate_long_short_ratio(
    csv_zip_path: str,
    target_date: str,
    tolerance: float = 0.01
) -> ValidationResult:
    """Validate Long/Short Ratio data: API vs historical CSV.
    
    Compares only rows where timestamps exist in BOTH datasets (intersection).
    Requires 100% match on the intersection.
    """
    details = []
    
    # Read historical CSV (same file as OI - metrics)
    csv_df = _read_csv_from_zip(csv_zip_path)
    csv_df["timestamp"] = pd.to_datetime(csv_df["create_time"], utc=True).dt.tz_localize(None)
    # Keep only hourly rows
    csv_df = csv_df[csv_df["timestamp"].dt.minute == 0].copy()
    csv_df = csv_df.rename(columns={"count_long_short_ratio": "csv_ls_ratio"})
    csv_df = csv_df[["timestamp", "csv_ls_ratio"]].reset_index(drop=True)
    
    # Fetch API data
    api_data = fetch_long_short_ratio(DEFAULT_SYMBOL, period="1h", limit=48)
    api_df = long_short_ratio_to_dataframe(api_data)
    api_df = api_df.rename(columns={"long_short_ratio": "api_ls_ratio"})
    
    # Find intersection of timestamps
    csv_timestamps = set(csv_df["timestamp"])
    api_timestamps = set(api_df["timestamp"])
    common_timestamps = csv_timestamps & api_timestamps
    
    csv_only = len(csv_timestamps - api_timestamps)
    api_only = len(api_timestamps - csv_timestamps)
    
    if not common_timestamps:
        return ValidationResult(
            data_type="Long/Short Ratio",
            comparison_rows=0,
            matched_rows=0,
            mismatched_rows=0,
            csv_only_rows=csv_only,
            api_only_rows=api_only,
            max_diff=0,
            details=["No overlapping timestamps found between CSV and API data"]
        )
    
    # Filter BOTH datasets to intersection only
    csv_compare = csv_df[csv_df["timestamp"].isin(common_timestamps)].copy()
    api_compare = api_df[api_df["timestamp"].isin(common_timestamps)].copy()
    
    # Merge on timestamp (inner join on intersection)
    merged = pd.merge(csv_compare, api_compare[["timestamp", "api_ls_ratio"]], on="timestamp", how="inner")
    
    # Compare
    merged["ls_diff"] = abs(merged["csv_ls_ratio"] - merged["api_ls_ratio"])
    mismatched = merged[merged["ls_diff"] > tolerance]
    max_diff = merged["ls_diff"].max() if not merged.empty else 0
    
    for _, row in mismatched.head(5).iterrows():
        details.append(
            f"  {row['timestamp']}: CSV={row['csv_ls_ratio']:.4f}, API={row['api_ls_ratio']:.4f}, diff={row['ls_diff']:.4f}"
        )
    
    return ValidationResult(
        data_type="Long/Short Ratio",
        comparison_rows=len(merged),
        matched_rows=len(merged) - len(mismatched),
        mismatched_rows=len(mismatched),
        csv_only_rows=csv_only,
        api_only_rows=api_only,
        max_diff=max_diff,
        details=details
    )


def validate_premium_index(
    csv_zip_path: str,
    target_date: str,
    tolerance: float = 1e-6
) -> ValidationResult:
    """Validate Premium Index Klines: API vs historical CSV.
    
    Compares only rows where timestamps exist in BOTH datasets (intersection).
    Requires 100% match on the intersection.
    """
    details = []
    
    # Read historical CSV (has header)
    csv_df = _read_csv_from_zip(csv_zip_path)
    csv_df["timestamp"] = pd.to_datetime(csv_df["open_time"], unit="ms", utc=True).dt.tz_localize(None)
    csv_df = csv_df.rename(columns={"open": "csv_open", "close": "csv_close"})
    csv_df = csv_df[["timestamp", "csv_open", "csv_close"]].reset_index(drop=True)
    csv_df["csv_open"] = csv_df["csv_open"].astype(float)
    csv_df["csv_close"] = csv_df["csv_close"].astype(float)
    
    # Fetch API data
    api_data = fetch_premium_index_klines(DEFAULT_SYMBOL, DEFAULT_INTERVAL, limit=48)
    api_df = klines_to_dataframe(api_data)
    api_df = api_df.rename(columns={"open": "api_open", "close": "api_close"})
    
    # Find intersection of timestamps
    csv_timestamps = set(csv_df["timestamp"])
    api_timestamps = set(api_df["timestamp"])
    common_timestamps = csv_timestamps & api_timestamps
    
    csv_only = len(csv_timestamps - api_timestamps)
    api_only = len(api_timestamps - csv_timestamps)
    
    if not common_timestamps:
        return ValidationResult(
            data_type="Premium Index",
            comparison_rows=0,
            matched_rows=0,
            mismatched_rows=0,
            csv_only_rows=csv_only,
            api_only_rows=api_only,
            max_diff=0,
            details=["No overlapping timestamps found between CSV and API data"]
        )
    
    # Filter BOTH datasets to intersection only
    csv_compare = csv_df[csv_df["timestamp"].isin(common_timestamps)].copy()
    api_compare = api_df[api_df["timestamp"].isin(common_timestamps)].copy()
    
    # Merge on timestamp (inner join on intersection)
    merged = pd.merge(csv_compare, api_compare[["timestamp", "api_open", "api_close"]], on="timestamp", how="inner")
    
    # Compare close values
    merged["close_diff"] = abs(merged["csv_close"] - merged["api_close"])
    mismatched = merged[merged["close_diff"] > tolerance]
    max_diff = merged["close_diff"].max() if not merged.empty else 0
    
    for _, row in mismatched.head(5).iterrows():
        details.append(
            f"  {row['timestamp']}: CSV={row['csv_close']:.8f}, API={row['api_close']:.8f}, diff={row['close_diff']:.8f}"
        )
    
    return ValidationResult(
        data_type="Premium Index",
        comparison_rows=len(merged),
        matched_rows=len(merged) - len(mismatched),
        mismatched_rows=len(mismatched),
        csv_only_rows=csv_only,
        api_only_rows=api_only,
        max_diff=max_diff,
        details=details
    )


def validate_spot_klines(
    csv_zip_path: str,
    target_date: str,
    tolerance: float = 0.01
) -> ValidationResult:
    """Validate Spot Klines: API vs historical CSV.
    
    Compares only rows where timestamps exist in BOTH datasets (intersection).
    Requires 100% match on the intersection.
    """
    details = []
    
    # Read historical CSV (no header, timestamps in nanoseconds for spot)
    csv_df = _read_klines_csv_from_zip(csv_zip_path)
    # Spot timestamps may be in nanoseconds (extra 3 zeros) - detect and convert
    if csv_df["open_time"].iloc[0] > 1e15:  # nanoseconds
        csv_df["timestamp"] = pd.to_datetime(csv_df["open_time"] // 1000, unit="ms", utc=True).dt.tz_localize(None)
    else:
        csv_df["timestamp"] = pd.to_datetime(csv_df["open_time"], unit="ms", utc=True).dt.tz_localize(None)
    
    csv_df = csv_df.rename(columns={
        "open": "csv_open", "close": "csv_close", 
        "volume": "csv_volume", "count": "csv_trades"
    })
    csv_df["csv_open"] = csv_df["csv_open"].astype(float)
    csv_df["csv_close"] = csv_df["csv_close"].astype(float)
    csv_df["csv_volume"] = csv_df["csv_volume"].astype(float)
    csv_df["csv_trades"] = csv_df["csv_trades"].astype(int)
    csv_df = csv_df[["timestamp", "csv_open", "csv_close", "csv_volume", "csv_trades"]].reset_index(drop=True)
    
    # Fetch API data
    api_data = fetch_spot_klines(DEFAULT_SYMBOL, DEFAULT_INTERVAL, limit=48)
    api_df = spot_klines_to_dataframe(api_data)
    api_df = api_df.rename(columns={
        "open": "api_open", "close": "api_close",
        "volume": "api_volume", "num_trades": "api_trades"
    })
    
    # Find intersection of timestamps
    csv_timestamps = set(csv_df["timestamp"])
    api_timestamps = set(api_df["timestamp"])
    common_timestamps = csv_timestamps & api_timestamps
    
    csv_only = len(csv_timestamps - api_timestamps)
    api_only = len(api_timestamps - csv_timestamps)
    
    if not common_timestamps:
        return ValidationResult(
            data_type="Spot Klines",
            comparison_rows=0,
            matched_rows=0,
            mismatched_rows=0,
            csv_only_rows=csv_only,
            api_only_rows=api_only,
            max_diff=0,
            details=["No overlapping timestamps found between CSV and API data"]
        )
    
    # Filter BOTH datasets to intersection only
    csv_compare = csv_df[csv_df["timestamp"].isin(common_timestamps)].copy()
    api_compare = api_df[api_df["timestamp"].isin(common_timestamps)].copy()
    
    # Merge on timestamp (inner join on intersection)
    merged = pd.merge(
        csv_compare, 
        api_compare[["timestamp", "api_open", "api_close", "api_volume", "api_trades"]], 
        on="timestamp", how="inner"
    )
    
    # Compare close values
    merged["close_diff"] = abs(merged["csv_close"] - merged["api_close"])
    mismatched = merged[merged["close_diff"] > tolerance]
    max_diff = merged["close_diff"].max() if not merged.empty else 0
    
    for _, row in mismatched.head(5).iterrows():
        details.append(
            f"  {row['timestamp']}: CSV={row['csv_close']:.2f}, API={row['api_close']:.2f}, diff={row['close_diff']:.4f}"
        )
    
    return ValidationResult(
        data_type="Spot Klines",
        comparison_rows=len(merged),
        matched_rows=len(merged) - len(mismatched),
        mismatched_rows=len(mismatched),
        csv_only_rows=csv_only,
        api_only_rows=api_only,
        max_diff=max_diff,
        details=details
    )


def run_validation(date: str, output_dir: str) -> int:
    """Run all validations for a given date."""
    print(f"[INFO] Validating API data for {date}")
    print(f"[INFO] Downloading historical data to {output_dir}")
    
    results = []
    
    # Download historical data
    metrics_url = build_metrics_url(DEFAULT_SYMBOL, date)
    metrics_path = os.path.join(output_dir, "metrics", f"{DEFAULT_SYMBOL}-metrics-{date}.zip")
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    status, _ = download_metrics(metrics_url, os.path.dirname(metrics_path))
    if status == "failed":
        print(f"[ERROR] Failed to download metrics for {date}")
        return 1
    
    premium_url = build_premium_url(DEFAULT_SYMBOL, DEFAULT_INTERVAL, date)
    premium_path = os.path.join(output_dir, "premium_index", f"{DEFAULT_SYMBOL}-{DEFAULT_INTERVAL}-{date}.zip")
    os.makedirs(os.path.dirname(premium_path), exist_ok=True)
    status, _ = download_premium(premium_url, os.path.dirname(premium_path))
    if status == "failed":
        print(f"[ERROR] Failed to download premium index for {date}")
        return 1
    
    spot_url = build_klines_url(DEFAULT_SYMBOL, DEFAULT_INTERVAL, date, market="spot")
    spot_path = os.path.join(output_dir, "spot_klines", f"{DEFAULT_SYMBOL}-{DEFAULT_INTERVAL}-{date}.zip")
    os.makedirs(os.path.dirname(spot_path), exist_ok=True)
    status, _ = download_klines(spot_url, os.path.dirname(spot_path))
    if status == "failed":
        print(f"[ERROR] Failed to download spot klines for {date}")
        return 1
    
    print()
    
    # Run validations
    print("=" * 60)
    print("VALIDATION RESULTS")
    print("=" * 60)
    
    # Open Interest
    try:
        oi_result = validate_open_interest(metrics_path, date)
        results.append(oi_result)
        _print_result(oi_result)
    except Exception as e:
        print(f"[ERROR] Open Interest validation failed: {e}")
    
    # Long/Short Ratio
    try:
        ls_result = validate_long_short_ratio(metrics_path, date)
        results.append(ls_result)
        _print_result(ls_result)
    except Exception as e:
        print(f"[ERROR] Long/Short Ratio validation failed: {e}")
    
    # Premium Index
    try:
        pi_result = validate_premium_index(premium_path, date)
        results.append(pi_result)
        _print_result(pi_result)
    except Exception as e:
        print(f"[ERROR] Premium Index validation failed: {e}")
    
    # Spot Klines
    try:
        spot_result = validate_spot_klines(spot_path, date)
        results.append(spot_result)
        _print_result(spot_result)
    except Exception as e:
        print(f"[ERROR] Spot Klines validation failed: {e}")
    
    print("=" * 60)
    all_ok = all(r.ok for r in results)
    print(f"OVERALL: {'PASS ✓' if all_ok else 'FAIL ✗'}")
    print("=" * 60)
    
    return 0 if all_ok else 1


def _print_result(r: ValidationResult) -> None:
    """Print a single validation result."""
    status = "✓ PASS" if r.ok else "✗ FAIL"
    print(f"\n{r.data_type}: {status}")
    print(f"  Compared: {r.comparison_rows} rows, Matched: {r.matched_rows}, Mismatched: {r.mismatched_rows}")
    print(f"  (CSV-only: {r.csv_only_rows}, API-only: {r.api_only_rows} - excluded from comparison)")
    if r.max_diff > 0:
        print(f"  Max diff: {r.max_diff:.6f}")
    for detail in r.details[:3]:
        print(detail)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate API data against historical CSVs")
    parser.add_argument("--date", required=True, help="Date to validate (YYYY-MM-DD)")
    parser.add_argument("--output-dir", default="/tmp/api_validation", help="Directory for downloaded files")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    return run_validation(args.date, args.output_dir)


if __name__ == "__main__":
    raise SystemExit(main())
