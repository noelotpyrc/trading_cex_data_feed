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

    from feed_binance_btcusdt_perp.api import Kline, klines_to_dataframe, compute_target_hour

    # Build out-of-order klines and ensure sorting + parsing
    def make_k(open_hour: str, open_val: str) -> Kline:
        t = pd.Timestamp(open_hour, tz='UTC')
        ot = int(t.timestamp() * 1000)
        ct = ot + 3600_000 - 1
        return Kline(open_time_ms=ot, open=open_val, high="100.1", low="99.9", close="100.0", volume="12.34", close_time_ms=ct)

    k3 = make_k('2024-01-01 03:00:00', '103.0')
    k1 = make_k('2024-01-01 01:00:00', '101.0')
    k2 = make_k('2024-01-01 02:00:00', '102.0')
    df = klines_to_dataframe([k3, k1, k2])
    assert list(df['timestamp'].astype(str)) == [
        '2024-01-01 01:00:00', '2024-01-01 02:00:00', '2024-01-01 03:00:00'
    ]
    assert df['open'].iloc[0] == 101.0 and df['open'].iloc[-1] == 103.0
    assert {'timestamp', 'open', 'high', 'low', 'close', 'volume'}.issubset(set(df.columns))

    # Compute target hour from a fixed now
    now_floor, target = compute_target_hour(pd.Timestamp('2024-01-15 14:23:45+00:00').to_pydatetime())
    assert str(now_floor) == '2024-01-15 14:00:00'
    assert str(target) == '2024-01-15 13:00:00'

    print('api tests OK')


if __name__ == '__main__':
    main()
