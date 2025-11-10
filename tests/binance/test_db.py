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

    from feed_binance_btcusdt_perp.db import ensure_table, append_row_if_absent, read_last_n_rows_ending_before, TABLE_NAME
    import duckdb  # type: ignore

    tmp_db = root / '.tmp' / 'feed_tests' / 'ohlcv_test.duckdb'
    tmp_db.parent.mkdir(parents=True, exist_ok=True)
    if tmp_db.exists():
        tmp_db.unlink()

    ensure_table(tmp_db)

    # Prepare timestamps
    t8 = pd.Timestamp('2024-01-01 08:00:00')
    t9 = pd.Timestamp('2024-01-01 09:00:00')
    t10 = pd.Timestamp('2024-01-01 10:00:00')

    def row(ts, o):
        return pd.Series({'timestamp': ts, 'open': o, 'high': o+2, 'low': o-2, 'close': o+1, 'volume': 10.0})

    append_row_if_absent(tmp_db, row(t8, 100.0))
    append_row_if_absent(tmp_db, row(t9, 110.0))

    # Read last 2 rows ending before t10
    df = read_last_n_rows_ending_before(tmp_db, 2, t10)
    assert list(df['timestamp'].astype(str)) == ['2024-01-01 08:00:00', '2024-01-01 09:00:00']

    # Append t10 and re-append to test idempotency
    append_row_if_absent(tmp_db, row(t10, 120.0))
    append_row_if_absent(tmp_db, row(t10, 120.0))

    con = duckdb.connect(str(tmp_db))
    try:
        cnt = con.execute(f'SELECT COUNT(*) FROM {TABLE_NAME}').fetchone()[0]
        assert cnt == 3
    finally:
        con.close()

    print('db tests OK')


if __name__ == '__main__':
    main()

