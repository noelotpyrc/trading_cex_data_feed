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

    from feed_binance_btcusdt_perp.persistence import PersistConfig, write_raw_snapshot

    df = pd.DataFrame([
        {"timestamp": pd.Timestamp('2024-01-01 00:00:00'), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 123.0}
    ])

    persist_root = root / '.tmp' / 'feed_tests' / 'artifacts'
    cfg = PersistConfig(persist_root, 'test_dataset')
    out = write_raw_snapshot(cfg, '20240101_000000Z', df)
    assert out.exists()

    df2 = pd.read_csv(out)
    assert list(df2.columns) == ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    assert len(df2) == 1

    print('persistence tests OK')


if __name__ == '__main__':
    main()

