#!/usr/bin/env python3
from __future__ import annotations

"""
Backfill recent BTCUSDT perp 1h OHLCV from Binance API into DuckDB.

Goals:
  1) Pull >24 recent records (default 48) to have overlap redundancy
  2) Read recent rows from DuckDB and identify overlap by timestamp
  3) Validate overlapping rows, then append only new, closed rows

Example:
  python feed_binance_btcusdt_perp/backfill_recent_from_api.py \
    --duckdb "/Volumes/Extreme SSD/trading_data/cex/db/binance_btcusdt_perp_ohlcv.duckdb" \
    --n-recent 48 --db-validate-rows 72
"""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

import pandas as pd

from cex_data_feed.binance.api import (
    fetch_klines,
    klines_to_dataframe,
    compute_target_hour,
)
from cex_data_feed.binance.db import (
    ensure_table,
    read_last_n_rows_ending_before,
    append_row_if_absent,
)
from cex_data_feed.binance.validation import validate_window as _vw


DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_INTERVAL = "1h"


@dataclass
class RunConfig:
    duckdb_path: Path
    n_recent: int = 48
    db_validate_rows: int = 72
    tolerance: float = 1e-8
    dry_run: bool = False
    debug: bool = False


def _filter_closed(api_df: pd.DataFrame, now_floor: pd.Timestamp) -> pd.DataFrame:
    if "_close_time" not in api_df.columns:
        raise RuntimeError("API DataFrame missing _close_time column")
    return api_df[api_df["_close_time"] <= now_floor - pd.Timedelta(milliseconds=1)].copy()


def run_once(cfg: RunConfig) -> int:
    ensure_table(cfg.duckdb_path)

    now_floor, target_hour = compute_target_hour()

    # Pull recent klines
    kl = fetch_klines(DEFAULT_SYMBOL, DEFAULT_INTERVAL, cfg.n_recent)
    api_df_all = klines_to_dataframe(kl)
    api_df = _filter_closed(api_df_all, now_floor)
    if api_df.empty:
        print("[ERROR] No closed candles returned from API in the requested window", file=sys.stderr)
        return 2

    last_closed_ts = api_df.iloc[-1]["timestamp"]
    if last_closed_ts != target_hour:
        print(
            f"[WARN] Last closed API bar {last_closed_ts} does not match target_hour {target_hour} (OK if within current hour)"
        )

    # Read DB window up to just before last_closed_ts+1h (so includes <= last_closed_ts)
    db_tail = read_last_n_rows_ending_before(cfg.duckdb_path, cfg.db_validate_rows, last_closed_ts + pd.Timedelta(hours=1))

    appended = 0

    if not db_tail.empty:
        # Identify overlap timestamps between API window and DB tail
        common_ts = sorted(set(api_df["timestamp"]).intersection(set(db_tail["timestamp"])))
        if not common_ts:
            print(
                f"[ERROR] No overlap between API window (min={api_df['timestamp'].min()}, max={api_df['timestamp'].max()}) "
                f"and DB tail (min={db_tail['timestamp'].min()}, max={db_tail['timestamp'].max()}). Consider increasing --n-recent/--db-validate-rows.",
                file=sys.stderr,
            )
            return 2

        # Build overlap series and validate using the shared validator, anchored at the last overlap as t
        api_overlap = api_df[api_df["timestamp"].isin(common_ts)].sort_values("timestamp").reset_index(drop=True)
        t_overlap = api_overlap.iloc[-1]["timestamp"]
        # Read DB rows ending at t_overlap - 1h for validation window size len(api_overlap)-1
        db_hist = read_last_n_rows_ending_before(cfg.duckdb_path, max(len(api_overlap) - 1, 0), t_overlap)

        v = _vw(api_overlap, db_hist, t_overlap, tolerance=cfg.tolerance)
        if not v.ok:
            print(f"[ERROR] Overlap validation failed: {v.reason}", file=sys.stderr)
            return 2

        # Additionally, ensure values for the last overlap row match DB within tolerance
        db_at_t = db_tail[db_tail["timestamp"] == t_overlap]
        if not db_at_t.empty:
            row_api = api_overlap.tail(1).iloc[0]
            row_db = db_at_t.tail(1).iloc[0]
            for col in ["open", "high", "low", "close", "volume"]:
                if abs(float(row_api[col]) - float(row_db[col])) > cfg.tolerance:
                    print(f"[ERROR] Mismatch at {t_overlap} in {col}: api={row_api[col]} db={row_db[col]}", file=sys.stderr)
                    return 2
        if cfg.debug:
            print(f"[INFO] overlap rows validated: {len(common_ts)}; last_overlap={t_overlap}")

        # Determine missing rows to append: strictly after DB max timestamp
        db_max_ts = db_tail["timestamp"].max()
        to_append = api_df[api_df["timestamp"] > db_max_ts].copy()
    else:
        # Bootstrap: DB empty, append entire API closed window
        to_append = api_df.copy()

    if to_append.empty:
        print("[INFO] No new rows to append; DB is up to date vs API window")
        return 0

    # Append sequentially to preserve order
    for _, row in to_append.iterrows():
        if cfg.dry_run:
            if cfg.debug:
                print("[DRY-RUN] Would append:", row.to_dict())
        else:
            append_row_if_absent(cfg.duckdb_path, row)
        appended += 1

    print(
        f"backfill_ok appended={appended} api_window=[{api_df['timestamp'].min()}..{api_df['timestamp'].max()}] "
        f"last_closed={last_closed_ts} target_hour={target_hour}"
    )
    return 0


def parse_args(argv: Optional[list[str]] = None) -> RunConfig:
    p = argparse.ArgumentParser(description="Backfill recent 1h BTCUSDT perp OHLCV from API into DuckDB")
    p.add_argument("--duckdb", type=Path, required=True, help="Path to DuckDB DB file")
    p.add_argument("--n-recent", type=int, default=48, help=">24 recent bars to pull from API (default: 48)")
    p.add_argument("--db-validate-rows", type=int, default=72, help="Rows to read from DB tail for overlap validation")
    p.add_argument("--tolerance", type=float, default=1e-8, help="Absolute tolerance for overlap validation")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args(argv)
    return RunConfig(
        duckdb_path=args.duckdb,
        n_recent=args.n_recent,
        db_validate_rows=args.db_validate_rows,
        tolerance=float(args.tolerance),
        dry_run=bool(args.dry_run),
        debug=bool(args.debug),
    )


def main(argv: Optional[list[str]] = None) -> int:
    cfg = parse_args(argv)
    try:
        return run_once(cfg)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        if cfg.debug:
            raise
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
