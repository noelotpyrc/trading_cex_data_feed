#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


def _proj_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _df_from_hours(hours, base_open=100.0):
    rows = []
    for i, h in enumerate(hours):
        ts = pd.Timestamp(h)
        o = base_open + i
        rows.append({
            'timestamp': ts,
            'open': o,
            'high': o + 2,
            'low': o - 2,
            'close': o + 1,
            'volume': 10.0 + i,
        })
    return pd.DataFrame(rows)


def main() -> None:
    root = _proj_root()
    if str(root) not in sys.path:
        sys.path.append(str(root))

    from feed_binance_btcusdt_perp.validation import validate_window

    t = pd.Timestamp('2024-01-01 10:00:00')
    api_df = _df_from_hours(['2024-01-01 08:00:00', '2024-01-01 09:00:00', '2024-01-01 10:00:00'])
    db_df = _df_from_hours(['2024-01-01 08:00:00', '2024-01-01 09:00:00'])
    res = validate_window(api_df, db_df, t)
    assert res.ok and res.validated_rows == 2

    # Value mismatch
    db_bad = db_df.copy()
    db_bad.loc[0, 'open'] = db_bad.loc[0, 'open'] + 0.01
    res2 = validate_window(api_df, db_bad, t, tolerance=1e-9)
    assert not res2.ok

    # Timestamp mismatch
    db_ts_bad = _df_from_hours(['2024-01-01 07:00:00', '2024-01-01 08:00:00'])
    res3 = validate_window(api_df, db_ts_bad, t)
    assert not res3.ok

    print('validation tests OK')


if __name__ == '__main__':
    main()

