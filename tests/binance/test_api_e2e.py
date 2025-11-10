#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
from urllib.error import URLError

import numpy as np
import pandas as pd


def _proj_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _assert_hourly(df: pd.DataFrame) -> None:
    ts = df["timestamp"].sort_values().reset_index(drop=True)
    diffs = ts.diff().dropna().dt.total_seconds().values
    assert (diffs == 3600).all(), f"non-hourly spacing detected: {diffs}"


def main() -> None:
    parser = argparse.ArgumentParser(description="E2E real API test (Binance klines)")
    parser.add_argument("--run", action="store_true", help="Execute real network call to Binance")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1h")
    parser.add_argument("--limit", type=int, default=6)
    args = parser.parse_args()

    if not args.run:
        print("SKIP: pass --run to execute real API call")
        return

    root = _proj_root()
    if str(root) not in sys.path:
        sys.path.append(str(root))

    from feed_binance_btcusdt_perp.api import fetch_klines, klines_to_dataframe

    try:
        kl = fetch_klines(args.symbol, args.interval, args.limit)
    except URLError as e:
        print(f"SKIP: network error {e}")
        return

    assert len(kl) == args.limit, f"expected {args.limit} rows, got {len(kl)}"
    df = klines_to_dataframe(kl)
    print(df)
    assert not df.empty
    # Column checks
    assert set(["timestamp", "open", "high", "low", "close", "volume"]).issubset(df.columns)
    # Dtypes convertible
    for c in ["open", "high", "low", "close", "volume"]:
        assert pd.api.types.is_float_dtype(df[c]), f"{c} not float dtype"
        assert np.isfinite(df[c]).all(), f"{c} contains non-finite values"
    # Hourly spacing
    _assert_hourly(df)
    # Close time sanity if available
    if "_close_time" in df.columns:
        assert (df["_close_time"] > df["timestamp"]).all()
        # within 1 hour window (allow equality due to ms truncation)
        delta = (df["_close_time"] - df["timestamp"]).dt.total_seconds()
        assert ((delta >= 3599) & (delta <= 3600)).all(), f"close_time offsets not ~1h: {delta.values}"

    # Last row recency sanity (should not be in the far future)
    now_floor = pd.Timestamp(datetime.now(timezone.utc)).floor("h").tz_convert(None)
    assert df["timestamp"].iloc[-1] <= now_floor, "last kline timestamp beyond current hour boundary"

    # Extra diagnostics: show which row would be used as the last CLOSED row
    target_hour = now_floor - pd.Timedelta(hours=1)
    last_api = df.tail(1).iloc[0]
    is_last_closed = last_api["_close_time"] <= now_floor - pd.Timedelta(milliseconds=1)
    print(
        f"last_api_row ts={last_api['timestamp']} close_time={last_api['_close_time']} closed={is_last_closed}"
    )
    closed_df = df[df["_close_time"] <= now_floor - pd.Timedelta(milliseconds=1)]
    if closed_df.empty:
        print("no closed candles in window; feed would wait for closure")
    else:
        chosen = closed_df.tail(1).iloc[0]
        matches_target = chosen["timestamp"] == target_hour
        print(
            f"feed_last_closed_row ts={chosen['timestamp']} close_time={chosen['_close_time']} target_hour={target_hour} matches_target={matches_target}"
        )

    print("api e2e test OK: rows=", len(df))


if __name__ == "__main__":
    main()
