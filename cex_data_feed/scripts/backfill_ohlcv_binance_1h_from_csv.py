#!/usr/bin/env python3
from __future__ import annotations

"""
Backfill Binance BTCUSDT Perp 1h OHLCV into DuckDB from a merged CSV.

Input CSV is expected to come from Binance Vision monthly ZIPs merged into one
file with the standard Binance kline header:
  open_time,open,high,low,close,volume,close_time,quote_asset_volume,
  number_of_trades,taker_buy_base_asset_volume,taker_buy_quote_asset_volume,ignore

Essential steps:
  - Inspect: rows, date range, duplicates, hourly continuity, close_time sanity
  - Clean/transform: parse timestamps (UTC-naive), numeric types, sort, dedupe
  - Optional range filter via --start/--end
  - Insert: append-only into DuckDB table, skipping existing timestamps

Usage example:
  python feed_binance_btcusdt_perp/backfill_ohlcv_binance_1h_from_csv.py \
    --csv "/Volumes/Extreme SSD/trading_data/cex/ohlvc/binance_btcusdt_perp_1h/merged.csv" \
    --duckdb "/Volumes/Extreme SSD/trading_data/cex/db/binance_btcusdt_perp_ohlcv.duckdb" \
    --stop-on-gap
"""

import argparse
from pathlib import Path
from typing import Optional

import duckdb  # type: ignore
import numpy as np
import pandas as pd

from db import ensure_table, TABLE_NAME


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


def _read_csv_with_header_detection(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    cols = [c.strip().lower() for c in df.columns]
    if "open_time" in cols:
        # Normalize column cases
        df.columns = [c.strip().lower() for c in df.columns]
        return df
    # Try without header
    df = pd.read_csv(path, header=None, names=BINANCE_KLINE_HEADER)
    return df


def inspect_dataframe(df: pd.DataFrame) -> None:
    print(f"[INSPECT] rows={len(df):,}")
    if "open_time" not in df.columns:
        print("[WARN] 'open_time' column missing; unexpected format")
        return
    ts = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    if ts.empty:
        print("[INSPECT] empty timestamp series")
        return
    print(f"[INSPECT] ts_range: {ts.iloc[0]} .. {ts.iloc[-1]}")
    # Duplicates
    dup_cnt = ts.duplicated().sum()
    if dup_cnt:
        print(f"[INSPECT] duplicate timestamps: {dup_cnt}")
    # Hourly spacing check
    diffs = ts.sort_values().diff().dropna().dt.total_seconds().values
    if diffs.size:
        ok = np.all(diffs == 3600)
        gaps = int((diffs != 3600).sum())
        print(f"[INSPECT] hourly_continuous={ok} gaps={gaps}")
    # close_time sanity
    if "close_time" in df.columns:
        ct = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
        delta = (ct - ts).dt.total_seconds()
        if not delta.empty:
            ok_close = bool(((delta >= 3599) & (delta <= 3600)).all())
            print(f"[INSPECT] close_time ~1h after open_time: {ok_close}")


def clean_transform(
    df: pd.DataFrame,
    *,
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    # Normalize column names
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    required = {"open_time", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"missing required columns in CSV: {sorted(missing)}")

    out = pd.DataFrame({
        "timestamp": pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert("UTC").dt.tz_localize(None),
        "open": pd.to_numeric(df["open"], errors="coerce"),
        "high": pd.to_numeric(df["high"], errors="coerce"),
        "low": pd.to_numeric(df["low"], errors="coerce"),
        "close": pd.to_numeric(df["close"], errors="coerce"),
        "volume": pd.to_numeric(df["volume"], errors="coerce"),
    })

    # Drop rows with NaNs in required fields
    before = len(out)
    out = out.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"]).copy()
    dropped_nan = before - len(out)
    if dropped_nan:
        print(f"[CLEAN] dropped rows with NaNs: {dropped_nan}")

    # Sort and dedupe by timestamp
    out = out.sort_values("timestamp").reset_index(drop=True)
    before = len(out)
    out = out.drop_duplicates(subset=["timestamp"], keep="first")
    dups = before - len(out)
    if dups:
        print(f"[CLEAN] dropped duplicate timestamps: {dups}")

    # Optional range filter
    if start is not None:
        out = out[out["timestamp"] >= start]
    if end is not None:
        out = out[out["timestamp"] <= end]
    out = out.reset_index(drop=True)

    # Hourly continuity check
    diffs = out["timestamp"].diff().dropna().dt.total_seconds().values
    gaps = int((diffs != 3600).sum()) if diffs.size else 0
    print(f"[CHECK] rows={len(out):,} range={out['timestamp'].iloc[0]}..{out['timestamp'].iloc[-1]} gaps={gaps}")

    # Close time sanity if present
    if "close_time" in df.columns:
        ct = pd.to_datetime(df.loc[out.index, "close_time"], unit="ms", utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
        delta = (ct.values - out["timestamp"].values).astype("timedelta64[s]").astype(int)
        close_bad = int(((delta < 3599) | (delta > 3600)).sum())
        if close_bad:
            print(f"[WARN] rows with unexpected close_time delta: {close_bad}")

    return out


def insert_into_duckdb(db_path: Path, df: pd.DataFrame) -> int:
    ensure_table(db_path)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("SET TimeZone='UTC';")
        con.register("tmp_df", df)
        # Count missing rows relative to DB (use NOT EXISTS for compatibility)
        to_insert = con.execute(
            f"""
            SELECT COUNT(*)
            FROM tmp_df t
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_NAME} d WHERE d.timestamp = t.timestamp
            )
            """
        ).fetchone()[0]
        # Insert only missing (NOT EXISTS is broadly supported)
        con.execute(
            f"""
            INSERT INTO {TABLE_NAME} (timestamp, open, high, low, close, volume)
            SELECT t.timestamp, t.open, t.high, t.low, t.close, t.volume
            FROM tmp_df t
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_NAME} d WHERE d.timestamp = t.timestamp
            )
            """
        )
        con.unregister("tmp_df")
        return int(to_insert)
    finally:
        con.close()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill Binance BTCUSDT Perp 1h OHLCV into DuckDB from CSV")
    p.add_argument(
        "--csv",
        type=Path,
        default=Path("/Volumes/Extreme SSD/trading_data/cex/ohlvc/binance_btcusdt_perp_1h/merged.csv"),
        help="Path to merged CSV file",
    )
    p.add_argument("--duckdb", type=Path, required=True, help="Path to DuckDB file to backfill into")
    p.add_argument("--start", type=str, default=None, help="Start timestamp (inclusive), e.g., 2020-01-01 00:00:00")
    p.add_argument("--end", type=str, default=None, help="End timestamp (inclusive)")
    p.add_argument("--stop-on-gap", action="store_true", help="Abort if hourly gaps are detected after cleaning")
    p.add_argument("--dry-run", action="store_true", help="Inspect and validate only; do not write to DB")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    df_raw = _read_csv_with_header_detection(args.csv)
    inspect_dataframe(df_raw)

    start = pd.to_datetime(args.start, utc=True).tz_convert("UTC").tz_localize(None) if args.start else None
    end = pd.to_datetime(args.end, utc=True).tz_convert("UTC").tz_localize(None) if args.end else None

    df = clean_transform(df_raw, start=start, end=end)

    # Post-clean continuity check
    diffs = df["timestamp"].diff().dropna().dt.total_seconds().values
    gaps = int((diffs != 3600).sum()) if diffs.size else 0
    if gaps and args.stop_on_gap:
        print(f"[ERROR] Hourly gaps detected after cleaning: {gaps}. Rerun without --stop-on-gap to continue.")
        return 2

    if args.dry_run:
        print("[DRY-RUN] Skipping DB insert.")
        return 0

    inserted = insert_into_duckdb(args.duckdb, df)
    print(f"[INFO] Inserted {inserted} new rows into {TABLE_NAME} at {args.duckdb}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
