from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from .api import fetch_klines, klines_to_dataframe, compute_target_hour
from .db import (
    ensure_table,
    read_last_n_rows_ending_before,
    append_row_if_absent,
    coverage_stats,
)
from .persistence import PersistConfig, now_utc_run_id, write_raw_snapshot
from .validation import validate_window


DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_INTERVAL = "1h"


@dataclass
class RunConfig:
    n_recent: int
    duckdb_path: Path
    persist_dir: Path
    dataset_slug: str
    dry_run: bool = False
    debug: bool = False
    catch_up: bool = False


def run_once(cfg: RunConfig) -> int:
    # Ensure DB table exists
    ensure_table(cfg.duckdb_path)

    # Compute times
    now_floor, target_hour = compute_target_hour()

    # Pull recent klines
    klines = fetch_klines(DEFAULT_SYMBOL, DEFAULT_INTERVAL, cfg.n_recent)
    api_df = klines_to_dataframe(klines)

    # Use only closed candles: close_time strictly before now_floor
    if "_close_time" not in api_df.columns:
        print("[ERROR] Missing _close_time column in API DataFrame", file=sys.stderr)
        return 2
    closed_df = api_df[api_df["_close_time"] <= now_floor - pd.Timedelta(milliseconds=1)].copy()
    if closed_df.empty:
        print("[ERROR] No closed candles in API response window", file=sys.stderr)
        return 2
    # Sanity: the last CLOSED row must be the target hour
    if closed_df.iloc[-1]["timestamp"] != target_hour:
        print(
            f"[ERROR] Last closed row {closed_df.iloc[-1]['timestamp']} does not equal target_hour {target_hour}",
            file=sys.stderr,
        )
        return 2

    # Persist raw snapshot regardless of validation result
    run_id = now_utc_run_id()
    persist_cfg = PersistConfig(cfg.persist_dir, cfg.dataset_slug)
    raw_path = write_raw_snapshot(persist_cfg, run_id, api_df)

    appended = 0
    if cfg.catch_up:
        # Catch-up mode: validate overlap and append all missing closed rows
        cov = coverage_stats(cfg.duckdb_path)
        if cov is None:
            # Bootstrap: DB empty, append entire closed window
            to_append = closed_df.copy()
        else:
            _, db_max_ts, _ = cov
            # Build overlap against DB tail up to db_max_ts
            api_overlap = closed_df[closed_df["timestamp"] <= db_max_ts].copy()
            if api_overlap.empty:
                print(
                    "[ERROR] No overlap between API closed window and DB. Increase --n-recent or backfill first.",
                    file=sys.stderr,
                )
                return 2
            # Validate overlap anchored at last overlap timestamp
            t_overlap = api_overlap.iloc[-1]["timestamp"]
            k = min(len(api_overlap), max(cfg.n_recent - 1, 1))
            api_tail_for_val = api_overlap.tail(k).reset_index(drop=True)
            db_hist = read_last_n_rows_ending_before(cfg.duckdb_path, len(api_tail_for_val) - 1, t_overlap)
            v = validate_window(api_tail_for_val, db_hist, t_overlap)
            if not v.ok:
                print(f"[ERROR] overlap validation failed: {v.reason}", file=sys.stderr)
                return 2
            # Append rows strictly after DB max timestamp
            to_append = closed_df[closed_df["timestamp"] > db_max_ts].copy()

        if to_append.empty:
            if cfg.debug:
                print("[INFO] Catch-up: DB is up to date; nothing to append")
        else:
            for _, row in to_append.iterrows():
                if cfg.dry_run:
                    if cfg.debug:
                        print("[DRY-RUN] Would append:", row.to_dict())
                else:
                    append_row_if_absent(cfg.duckdb_path, row)
                appended += 1
    else:
        # Read DB window for validation: last N-1 rows ending at t-1
        db_window = read_last_n_rows_ending_before(cfg.duckdb_path, cfg.n_recent - 1, target_hour)

        # Validate single-hour append
        v = validate_window(closed_df, db_window, target_hour)

        allow_bootstrap = False
        if not v.ok and len(db_window) == 0:
            allow_bootstrap = True
            if cfg.debug:
                print("[INFO] bootstrap: no DB history; appending target hour without full validation")

        if v.ok or allow_bootstrap:
            # Append only bar at t
            row_t = closed_df.tail(1).iloc[0]
            if cfg.dry_run:
                if cfg.debug:
                    print("[DRY-RUN] Would append:", row_t.to_dict())
            else:
                append_row_if_absent(cfg.duckdb_path, row_t)
            appended = 1
        else:
            print(f"[WARN] validation failed: {v.reason}")

    # Log concise stats
    print(
        f"pulled={len(api_df)} validated={v.validated_rows if v.ok else 0} appended={appended} "
        f"target_hour={target_hour} raw={raw_path}"
    )

    return 0 if (v.ok or allow_bootstrap) else 1


def parse_args(argv: Optional[list[str]] = None) -> RunConfig:
    p = argparse.ArgumentParser(description="Binance BTCUSDT Perp 1H OHLCV feed")
    p.add_argument("--n-recent", type=int, default=6, help="Number of most recent bars to pull")
    p.add_argument("--duckdb", type=Path, required=True, help="Path to DuckDB file")
    p.add_argument("--persist-dir", type=Path, required=True, help="Directory root for artifacts")
    p.add_argument(
        "--dataset",
        type=str,
        default="binance_btcusdt_perp_1h",
        help="Dataset slug directory for artifacts",
    )
    p.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    p.add_argument("--debug", action="store_true", help="Verbose logging")
    p.add_argument("--catch-up", action="store_true", help="Append all missing closed bars in the API window after validating overlap")
    args = p.parse_args(argv)

    return RunConfig(
        n_recent=args.n_recent,
        duckdb_path=args.duckdb,
        persist_dir=args.persist_dir,
        dataset_slug=args.dataset,
        dry_run=args.dry_run,
        debug=args.debug,
        catch_up=args.catch_up,
    )


def main(argv: Optional[list[str]] = None) -> int:
    cfg = parse_args(argv)
    try:
        return run_once(cfg)
    except Exception as e:  # surface clear error message
        print(f"[ERROR] {e}", file=sys.stderr)
        if cfg.debug:
            raise
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
