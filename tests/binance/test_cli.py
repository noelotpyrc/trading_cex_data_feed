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
    from feed_binance_btcusdt_perp.cli import run_once, RunConfig
    import feed_binance_btcusdt_perp.cli as cli_mod

    # Fix the target hour to a known value via monkeypatching compute_target_hour
    now_floor = pd.Timestamp('2024-01-01 11:00:00')
    target = pd.Timestamp('2024-01-01 10:00:00')
    cli_mod.compute_target_hour = lambda: (now_floor, target)  # type: ignore

    # Stub fetch_klines to avoid network and to match target hour
    def make_k(ts: pd.Timestamp, open_val: float) -> api_mod.Kline:
        ot = int(ts.tz_localize('UTC').timestamp() * 1000)
        ct = ot + 3600_000 - 1  # closed before now_floor
        return api_mod.Kline(open_time_ms=ot, open=f"{open_val}", high=f"{open_val+2}", low=f"{open_val-2}", close=f"{open_val+1}", volume="10.0", close_time_ms=ct)

    def fake_fetch(symbol: str, interval: str, limit: int):
        return [
            make_k(pd.Timestamp('2024-01-01 08:00:00'), 100.0),
            make_k(pd.Timestamp('2024-01-01 09:00:00'), 110.0),
            make_k(pd.Timestamp('2024-01-01 10:00:00'), 120.0),
        ]

    cli_mod.fetch_klines = fake_fetch  # type: ignore

    tmp_root = root / '.tmp' / 'feed_tests' / 'cli'
    tmp_root.mkdir(parents=True, exist_ok=True)
    db_path = tmp_root / 'ohlcv.duckdb'
    persist_dir = tmp_root / 'artifacts'

    rc = RunConfig(n_recent=3, duckdb_path=db_path, persist_dir=persist_dir, dataset_slug='cli_dataset', dry_run=True, debug=True)
    code = run_once(rc)
    # DB is empty, so bootstrap path returns 0
    assert code == 0

    # Raw snapshot should exist
    files = list((persist_dir / 'cli_dataset').glob('*_api_pull.csv'))
    assert files, 'raw snapshot not written'

    print('cli tests OK')


if __name__ == '__main__':
    main()

