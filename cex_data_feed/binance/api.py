from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json

import pandas as pd


BINANCE_FAPI = "https://fapi.binance.com"


@dataclass(frozen=True)
class Kline:
    open_time_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time_ms: int


def _build_klines_url(symbol: str, interval: str, limit: int) -> str:
    qs = urlencode({"symbol": symbol, "interval": interval, "limit": limit})
    return f"{BINANCE_FAPI}/fapi/v1/klines?{qs}"


def fetch_klines(symbol: str, interval: str, limit: int) -> List[Kline]:
    """Fetch recent klines from Binance Futures API.

    Returns a list of Kline with string price/volume fields as returned by the API.
    """
    url = _build_klines_url(symbol, interval, limit)
    req = Request(url, headers={"User-Agent": "ohlcv-feed/1.0"})
    with urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read())
    klines: List[Kline] = []
    for row in payload:
        # Row format per Binance docs
        # [ openTime, open, high, low, close, volume, closeTime, quoteAssetVolume,
        #   numberOfTrades, takerBuyBaseAssetVolume, takerBuyQuoteAssetVolume, ignore ]
        klines.append(
            Kline(
                open_time_ms=int(row[0]),
                open=str(row[1]),
                high=str(row[2]),
                low=str(row[3]),
                close=str(row[4]),
                volume=str(row[5]),
                close_time_ms=int(row[6]),
            )
        )
    return klines


def klines_to_dataframe(klines: List[Kline]) -> pd.DataFrame:
    """Map raw klines into canonical DataFrame: timestamp, open, high, low, close, volume.

    - timestamp: pandas datetime64[ns] (UTC, naive by convention)
    - numerical columns: float64
    - sorted ascending by timestamp
    """
    if not klines:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]).astype(
            {"timestamp": "datetime64[ns]", "open": float, "high": float, "low": float, "close": float, "volume": float}
        )
    df = pd.DataFrame(
        [
            {
                "timestamp": pd.to_datetime(k.open_time_ms, unit="ms", utc=True).tz_convert(None),
                "open": float(k.open),
                "high": float(k.high),
                "low": float(k.low),
                "close": float(k.close),
                "volume": float(k.volume),
                "_close_time": pd.to_datetime(k.close_time_ms, unit="ms", utc=True).tz_convert(None),
            }
            for k in klines
        ]
    )
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    return df


def compute_target_hour(now: datetime | None = None) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Compute now_floor (hour boundary) and target_hour (latest fully closed hour).

    Returns (now_floor, target_hour) as pandas Timestamps (UTC-naive by convention).
    """
    now = now or datetime.now(timezone.utc)
    now_floor = pd.Timestamp(now).floor("h").tz_convert(None)
    target_hour = now_floor - pd.Timedelta(hours=1)
    return now_floor, target_hour
