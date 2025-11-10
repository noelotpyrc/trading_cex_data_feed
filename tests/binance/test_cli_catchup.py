#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


def _proj_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> None:
    root = _proj_root()
    if str(root) not in sys.path:
        sys.path.append(str(root))

    from feed_binance_btcusdt_perp import api as api_mod
    import feed_binance_btcusdt_perp.cli as cli_mod
    from feed_binance_btcusdt_perp.db import ensure_table, append_row_if_absent, coverage_stats

    # Fix the target hour context
    now_floor = pd.Timestamp('2024-01-01 12:00:00')
    target = pd.Timestamp('2024-01-01 11:00:00')
    cli_mod.compute_target_hour = lambda: (now_floor, target)  # type: ignore

    # Stub fetch_klines to produce 08:00, 09:00, 10:00, 11:00
    def make_k(ts: pd.Timestamp, open_val: float) -> api_mod.Kline:
        ot = int(ts.tz_localize('UTC').timestamp() * 1000)
        ct = ot + 3600_000 - 1
        return api_mod.Kline(open_time_ms=ot, open=f"{open_val}", high=f"{open_val+2}", low=f"{open_val-2}", close=f"{open_val+1}", volume="10.0", close_time_ms=ct)

    def fake_fetch(symbol: str, interval: str, limit: int):
        return [
            make_k(pd.Timestamp('2024-01-01 08:00:00'), 100.0),
            make_k(pd.Timestamp('2024-01-01 09:00:00'), 110.0),
            make_k(pd.Timestamp('2024-01-01 10:00:00'), 120.0),
            make_k(pd.Timestamp('2024-01-01 11:00:00'), 130.0),
        ]

    cli_mod.fetch_klines = fake_fetch  # type: ignore

    # Temp DB and artifacts
    tmp_root = root / '.tmp' / 'feed_tests' / 'cli_catchup'
    tmp_root.mkdir(parents=True, exist_ok=True)
    db_path = tmp_root / 'ohlcv.duckdb'
    persist_dir = tmp_root / 'artifacts'

    # Seed DB with 08:00 and 09:00 only
    ensure_table(db_path)
    def row(ts, o):
        return pd.Series({'timestamp': pd.Timestamp(ts), 'open': o, 'high': o+2, 'low': o-2, 'close': o+1, 'volume': 10.0})
    append_row_if_absent(db_path, row('2024-01-01 08:00:00', 100.0))
    append_row_if_absent(db_path, row('2024-01-01 09:00:00', 110.0))

    # Run catch-up (dry-run False so it writes to DB)
    rc = cli_mod.RunConfig(n_recent=4, duckdb_path=db_path, persist_dir=persist_dir, dataset_slug='cli_dataset', dry_run=False, debug=True, catch_up=True)
    code = cli_mod.run_once(rc)
    assert code == 0

    cov = coverage_stats(db_path)
    assert cov is not None
    tmin, tmax, cnt = cov
    assert str(tmin) == '2024-01-01 08:00:00'
    assert str(tmax) == '2024-01-01 11:00:00'
    assert cnt == 4, f"expected 4 rows after catch-up, got {cnt}"
    print('cli catch-up tests OK')


if __name__ == '__main__':
    main()

