#!/usr/bin/env python3
from __future__ import annotations

"""
Backfill recent BTCUSDT perp 1h OHLCV and additional metrics from Binance API into DuckDB.

Goals:
  1) Pull >24 recent records (default 48) to have overlap redundancy
  2) Read recent rows from DuckDB and identify overlap by timestamp
  3) Validate overlapping rows, then append only new, closed rows

Example:
  python cex_data_feed/scripts/backfill_recent_from_api.py \
    --duckdb "/Volumes/Extreme SSD/trading_data/cex/db/binance_btcusdt_perp_ohlcv.duckdb" \
    --n-recent 48 --db-validate-rows 72

  # Fetch all data types
  python cex_data_feed/scripts/backfill_recent_from_api.py \
    --duckdb "/path/to/db.duckdb" --all
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
    fetch_open_interest_hist,
    open_interest_hist_to_dataframe,
    fetch_long_short_ratio,
    long_short_ratio_to_dataframe,
    fetch_premium_index_klines,
    fetch_spot_klines,
    spot_klines_to_dataframe,
)
from cex_data_feed.binance.db import (
    ensure_table,
    read_last_n_rows_ending_before,
    append_row_if_absent,
    ensure_table_open_interest,
    append_open_interest_if_absent,
    read_last_n_open_interest,
    ensure_table_long_short_ratio,
    append_long_short_ratio_if_absent,
    read_last_n_long_short_ratio,
    ensure_table_premium_index,
    append_premium_index_if_absent,
    read_last_n_premium_index,
    ensure_table_spot_ohlcv,
    append_spot_ohlcv_if_absent,
    read_last_n_spot_ohlcv,
)
from cex_data_feed.binance.validation import validate_window as _vw


import yaml


DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_INTERVAL = "1h"

# Default config file path
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def load_config(config_path: Path) -> dict:
    """Load configuration from YAML file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)


@dataclass
class RunConfig:
    # DB paths (loaded from config or defaults)
    ohlcv_db: Path
    open_interest_db: Path
    long_short_ratio_db: Path
    premium_index_db: Path
    spot_ohlcv_db: Path
    # Settings
    n_recent: int = 48
    db_validate_rows: int = 72
    tolerance: float = 1e-8
    dry_run: bool = False
    debug: bool = False
    # Data type flags
    include_open_interest: bool = False
    include_long_short_ratio: bool = False
    include_premium_index: bool = False
    include_spot_ohlcv: bool = False

    def get_ohlcv_db(self) -> Path:
        return self.ohlcv_db

    def get_open_interest_db(self) -> Path:
        return self.open_interest_db

    def get_long_short_ratio_db(self) -> Path:
        return self.long_short_ratio_db

    def get_premium_index_db(self) -> Path:
        return self.premium_index_db

    def get_spot_ohlcv_db(self) -> Path:
        return self.spot_ohlcv_db



def _filter_closed(api_df: pd.DataFrame, now_floor: pd.Timestamp) -> pd.DataFrame:
    """Filter to only closed candles (close_time < now_floor)."""
    if "_close_time" not in api_df.columns:
        raise RuntimeError("API DataFrame missing _close_time column")
    return api_df[api_df["_close_time"] <= now_floor - pd.Timedelta(milliseconds=1)].copy()


def _filter_closed_by_timestamp(api_df: pd.DataFrame, now_floor: pd.Timestamp) -> pd.DataFrame:
    """Filter to only complete hours (timestamp < now_floor) for APIs without close_time."""
    return api_df[api_df["timestamp"] < now_floor].copy()


def run_once(cfg: RunConfig) -> int:
    # Ensure parent directories exist for each DB
    for db_path in [cfg.ohlcv_db, cfg.open_interest_db, cfg.long_short_ratio_db, 
                    cfg.premium_index_db, cfg.spot_ohlcv_db]:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_table(cfg.get_ohlcv_db())

    now_floor, target_hour = compute_target_hour()

    # Pull recent klines (always runs - main OHLCV)
    kl = fetch_klines(DEFAULT_SYMBOL, DEFAULT_INTERVAL, cfg.n_recent)
    api_df_all = klines_to_dataframe(kl)
    
    # Add snapshot_time = close time (open + 1h for 1h candles)
    api_df_all["snapshot_time"] = api_df_all["timestamp"] + pd.Timedelta(hours=1)
    
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
    db_tail = read_last_n_rows_ending_before(cfg.get_ohlcv_db(), cfg.db_validate_rows, last_closed_ts + pd.Timedelta(hours=1))

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
        db_hist = read_last_n_rows_ending_before(cfg.get_ohlcv_db(), max(len(api_overlap) - 1, 0), t_overlap)

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
    else:
        # Append sequentially to preserve order
        for _, row in to_append.iterrows():
            if cfg.dry_run:
                if cfg.debug:
                    print("[DRY-RUN] Would append:", row.to_dict())
            else:
                append_row_if_absent(cfg.get_ohlcv_db(), row)
            appended += 1

        ts_min = to_append['timestamp'].min()
        ts_max = to_append['timestamp'].max()
        print(
            f"[INFO] Perp OHLCV: appended={appended} range=[{ts_min}..{ts_max}]"
        )

    # --- Open Interest ---
    if cfg.include_open_interest:
        _backfill_open_interest(cfg, now_floor)

    # --- Long/Short Ratio ---
    if cfg.include_long_short_ratio:
        _backfill_long_short_ratio(cfg, now_floor)

    # --- Premium Index ---
    if cfg.include_premium_index:
        _backfill_premium_index(cfg, now_floor)

    # --- Spot OHLCV ---
    if cfg.include_spot_ohlcv:
        _backfill_spot_ohlcv(cfg, now_floor)

    return 0


def _backfill_long_short_ratio(cfg: RunConfig, now_floor: pd.Timestamp) -> None:
    """Backfill long/short ratio data.
    
    Note: API timestamp is the snapshot time. We store:
      - snapshot_time = API timestamp (actual snapshot)
      - timestamp = snapshot_time - 1h (aligned with OHLCV candle open time)
    """
    db_path = cfg.get_long_short_ratio_db()
    ensure_table_long_short_ratio(db_path)

    ratios = fetch_long_short_ratio(DEFAULT_SYMBOL, period="1h", limit=cfg.n_recent)
    api_df = long_short_ratio_to_dataframe(ratios)
    
    # Transform timestamps: API returns snapshot_time, we align to OHLCV candle
    api_df["snapshot_time"] = api_df["timestamp"]
    api_df["timestamp"] = api_df["snapshot_time"] - pd.Timedelta(hours=1)
    
    api_df = _filter_closed_by_timestamp(api_df, now_floor)

    if api_df.empty:
        print("[INFO] Long/Short Ratio: No closed data returned")
        return

    last_closed_ts = api_df.iloc[-1]["timestamp"]
    db_tail = read_last_n_long_short_ratio(db_path, cfg.db_validate_rows, last_closed_ts + pd.Timedelta(hours=1))

    if not db_tail.empty:
        db_max_ts = db_tail["timestamp"].max()
        to_append = api_df[api_df["timestamp"] > db_max_ts].copy()
    else:
        to_append = api_df.copy()

    if to_append.empty:
        print("[INFO] Long/Short Ratio: No new rows to append")
        return

    appended = 0
    for _, row in to_append.iterrows():
        if cfg.dry_run:
            if cfg.debug:
                print("[DRY-RUN] Would append long_short_ratio:", row.to_dict())
        else:
            append_long_short_ratio_if_absent(db_path, row)
        appended += 1

    ts_min = to_append['timestamp'].min()
    ts_max = to_append['timestamp'].max()
    print(f"[INFO] Long/Short Ratio: appended={appended} range=[{ts_min}..{ts_max}]")


def _backfill_open_interest(cfg: RunConfig, now_floor: pd.Timestamp) -> None:
    """Backfill open interest statistics.
    
    Note: API timestamp is the snapshot time. We store:
      - snapshot_time = API timestamp (actual snapshot)
      - timestamp = snapshot_time - 1h (aligned with OHLCV candle open time)
    """
    db_path = cfg.get_open_interest_db()
    ensure_table_open_interest(db_path)

    oi_list = fetch_open_interest_hist(DEFAULT_SYMBOL, period="1h", limit=cfg.n_recent)
    api_df = open_interest_hist_to_dataframe(oi_list)
    
    # Transform timestamps: API returns snapshot_time, we align to OHLCV candle
    api_df["snapshot_time"] = api_df["timestamp"]
    api_df["timestamp"] = api_df["snapshot_time"] - pd.Timedelta(hours=1)
    
    api_df = _filter_closed_by_timestamp(api_df, now_floor)

    if api_df.empty:
        print("[INFO] Open Interest: No closed data returned")
        return

    last_closed_ts = api_df.iloc[-1]["timestamp"]
    db_tail = read_last_n_open_interest(db_path, cfg.db_validate_rows, last_closed_ts + pd.Timedelta(hours=1))

    if not db_tail.empty:
        db_max_ts = db_tail["timestamp"].max()
        to_append = api_df[api_df["timestamp"] > db_max_ts].copy()
    else:
        to_append = api_df.copy()

    if to_append.empty:
        print("[INFO] Open Interest: No new rows to append")
        return

    appended = 0
    for _, row in to_append.iterrows():
        if cfg.dry_run:
            if cfg.debug:
                print("[DRY-RUN] Would append open_interest:", row.to_dict())
        else:
            append_open_interest_if_absent(db_path, row)
        appended += 1

    ts_min = to_append['timestamp'].min()
    ts_max = to_append['timestamp'].max()
    print(f"[INFO] Open Interest: appended={appended} range=[{ts_min}..{ts_max}]")


def _backfill_premium_index(cfg: RunConfig, now_floor: pd.Timestamp) -> None:
    """Backfill premium index klines.
    
    Note: This is OHLCV-like data. We store:
      - timestamp = API open_time (candle open)
      - snapshot_time = timestamp + 1h (candle close)
    """
    db_path = cfg.get_premium_index_db()
    ensure_table_premium_index(db_path)

    klines = fetch_premium_index_klines(DEFAULT_SYMBOL, DEFAULT_INTERVAL, cfg.n_recent)
    api_df = klines_to_dataframe(klines)
    
    # Add snapshot_time = close time (open + 1h for 1h candles)
    api_df["snapshot_time"] = api_df["timestamp"] + pd.Timedelta(hours=1)
    
    api_df = _filter_closed(api_df, now_floor)

    if api_df.empty:
        print("[INFO] Premium Index: No closed data returned")
        return

    last_closed_ts = api_df.iloc[-1]["timestamp"]
    db_tail = read_last_n_premium_index(db_path, cfg.db_validate_rows, last_closed_ts + pd.Timedelta(hours=1))

    if not db_tail.empty:
        db_max_ts = db_tail["timestamp"].max()
        to_append = api_df[api_df["timestamp"] > db_max_ts].copy()
    else:
        to_append = api_df.copy()

    if to_append.empty:
        print("[INFO] Premium Index: No new rows to append")
        return

    appended = 0
    for _, row in to_append.iterrows():
        if cfg.dry_run:
            if cfg.debug:
                print("[DRY-RUN] Would append premium_index:", row.to_dict())
        else:
            append_premium_index_if_absent(db_path, row)
        appended += 1

    ts_min = to_append['timestamp'].min()
    ts_max = to_append['timestamp'].max()
    print(f"[INFO] Premium Index: appended={appended} range=[{ts_min}..{ts_max}]")


def _backfill_spot_ohlcv(cfg: RunConfig, now_floor: pd.Timestamp) -> None:
    """Backfill spot OHLCV klines.
    
    Note: This is OHLCV-like data. We store:
      - timestamp = API open_time (candle open)
      - snapshot_time = timestamp + 1h (candle close)
    """
    db_path = cfg.get_spot_ohlcv_db()
    ensure_table_spot_ohlcv(db_path)

    klines = fetch_spot_klines(DEFAULT_SYMBOL, DEFAULT_INTERVAL, cfg.n_recent)
    api_df = spot_klines_to_dataframe(klines)
    
    # Add snapshot_time = close time (open + 1h for 1h candles)
    api_df["snapshot_time"] = api_df["timestamp"] + pd.Timedelta(hours=1)
    
    api_df = _filter_closed(api_df, now_floor)

    if api_df.empty:
        print("[INFO] Spot OHLCV: No closed data returned")
        return

    last_closed_ts = api_df.iloc[-1]["timestamp"]
    db_tail = read_last_n_spot_ohlcv(db_path, cfg.db_validate_rows, last_closed_ts + pd.Timedelta(hours=1))

    if not db_tail.empty:
        db_max_ts = db_tail["timestamp"].max()
        to_append = api_df[api_df["timestamp"] > db_max_ts].copy()
    else:
        to_append = api_df.copy()

    if to_append.empty:
        print("[INFO] Spot OHLCV: No new rows to append")
        return

    appended = 0
    for _, row in to_append.iterrows():
        if cfg.dry_run:
            if cfg.debug:
                print("[DRY-RUN] Would append spot_ohlcv:", row.to_dict())
        else:
            append_spot_ohlcv_if_absent(db_path, row)
        appended += 1

    ts_min = to_append['timestamp'].min()
    ts_max = to_append['timestamp'].max()
    print(f"[INFO] Spot OHLCV: appended={appended} range=[{ts_min}..{ts_max}]")


def parse_args(argv: Optional[list[str]] = None) -> RunConfig:
    p = argparse.ArgumentParser(
        description="Backfill recent 1h BTCUSDT data from API into DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default config file
  python -m cex_data_feed.scripts.backfill_recent_from_api --all

  # Use custom config file
  python -m cex_data_feed.scripts.backfill_recent_from_api --config /path/to/config.yaml --all

  # Legacy mode (single DB for OHLCV only, ignores config)
  python -m cex_data_feed.scripts.backfill_recent_from_api --duckdb /path/to/db.duckdb
"""
    )
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH,
                   help=f"Path to config YAML file (default: {DEFAULT_CONFIG_PATH})")
    # Legacy --duckdb argument for backward compatibility
    p.add_argument("--duckdb", type=Path, 
                   help="[LEGACY] Path to DuckDB file (overrides config, OHLCV only)")
    p.add_argument("--n-recent", type=int, help=">24 recent bars to pull from API (overrides config)")
    p.add_argument("--db-validate-rows", type=int, help="Rows to read from DB tail (overrides config)")
    p.add_argument("--tolerance", type=float, help="Absolute tolerance for validation (overrides config)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--debug", action="store_true")
    # Data type flags
    p.add_argument("--include-open-interest", action="store_true", help="Fetch Open Interest Statistics")
    p.add_argument("--include-long-short-ratio", action="store_true", help="Fetch Long/Short Ratio")
    p.add_argument("--include-premium-index", action="store_true", help="Fetch Premium Index Klines")
    p.add_argument("--include-spot-ohlcv", action="store_true", help="Fetch Spot OHLCV")
    p.add_argument("--all", action="store_true", help="Fetch all data types")
    args = p.parse_args(argv)

    include_all = args.all

    # Legacy mode: --duckdb overrides config
    if args.duckdb:
        # Use the provided path for OHLCV, derive others from same directory
        db_dir = args.duckdb.parent
        ohlcv_db = args.duckdb
        open_interest_db = db_dir / "binance_btcusdt_open_interest.duckdb"
        long_short_ratio_db = db_dir / "binance_btcusdt_long_short_ratio.duckdb"
        premium_index_db = db_dir / "binance_btcusdt_premium_index.duckdb"
        spot_ohlcv_db = db_dir / "binance_btcusdt_spot_ohlcv.duckdb"
        n_recent = args.n_recent or 48
        db_validate_rows = args.db_validate_rows or 72
        tolerance = args.tolerance if args.tolerance is not None else 1e-8
    else:
        # Load config from file
        config = load_config(args.config)
        db_paths = config.get("db_paths", {})
        backfill_cfg = config.get("backfill", {})
        ohlcv_db = Path(db_paths.get("ohlcv", "/tmp/binance_btcusdt_perp_ohlcv.duckdb"))
        open_interest_db = Path(db_paths.get("open_interest", "/tmp/binance_btcusdt_open_interest.duckdb"))
        long_short_ratio_db = Path(db_paths.get("long_short_ratio", "/tmp/binance_btcusdt_long_short_ratio.duckdb"))
        premium_index_db = Path(db_paths.get("premium_index", "/tmp/binance_btcusdt_premium_index.duckdb"))
        spot_ohlcv_db = Path(db_paths.get("spot_ohlcv", "/tmp/binance_btcusdt_spot_ohlcv.duckdb"))
        n_recent = args.n_recent or backfill_cfg.get("n_recent", 48)
        db_validate_rows = args.db_validate_rows or backfill_cfg.get("db_validate_rows", 72)
        tolerance = args.tolerance if args.tolerance is not None else backfill_cfg.get("tolerance", 1e-8)

    return RunConfig(
        ohlcv_db=ohlcv_db,
        open_interest_db=open_interest_db,
        long_short_ratio_db=long_short_ratio_db,
        premium_index_db=premium_index_db,
        spot_ohlcv_db=spot_ohlcv_db,
        n_recent=n_recent,
        db_validate_rows=db_validate_rows,
        tolerance=tolerance,
        dry_run=bool(args.dry_run),
        debug=bool(args.debug),
        include_open_interest=include_all or args.include_open_interest,
        include_long_short_ratio=include_all or args.include_long_short_ratio,
        include_premium_index=include_all or args.include_premium_index,
        include_spot_ohlcv=include_all or args.include_spot_ohlcv,
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
