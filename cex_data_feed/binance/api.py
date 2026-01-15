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
    quote_asset_volume: str | None = None
    num_trades: int = 0
    taker_buy_base_volume: str | None = None
    taker_buy_quote_volume: str | None = None


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
                quote_asset_volume=str(row[7]),
                num_trades=int(row[8]),
                taker_buy_base_volume=str(row[9]),
                taker_buy_quote_volume=str(row[10]),
            )
        )
    return klines


def klines_to_dataframe(klines: List[Kline]) -> pd.DataFrame:
    """Map raw klines into canonical DataFrame.

    Columns: timestamp, open, high, low, close, volume, quote_asset_volume,
             num_trades, taker_buy_base_volume, taker_buy_quote_volume, _close_time

    - timestamp: pandas datetime64[ns] (UTC, naive by convention)
    - numerical columns: float64 (num_trades: int)
    - sorted ascending by timestamp
    """
    if not klines:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_asset_volume",
                "num_trades",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
            ]
        ).astype(
            {
                "timestamp": "datetime64[ns]",
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
                "quote_asset_volume": float,
                "num_trades": int,
                "taker_buy_base_volume": float,
                "taker_buy_quote_volume": float,
            }
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
                "quote_asset_volume": float(k.quote_asset_volume) if k.quote_asset_volume is not None else None,
                "num_trades": k.num_trades,
                "taker_buy_base_volume": float(k.taker_buy_base_volume) if k.taker_buy_base_volume is not None else None,
                "taker_buy_quote_volume": float(k.taker_buy_quote_volume) if k.taker_buy_quote_volume is not None else None,
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


# -----------------------------------------------------------------------------
# Open Interest
# -----------------------------------------------------------------------------

BINANCE_SPOT_API = "https://api.binance.com"


@dataclass(frozen=True)
class OpenInterest:
    symbol: str
    open_interest: float
    time_ms: int


def fetch_open_interest(symbol: str) -> OpenInterest:
    """Fetch current open interest from Binance Futures API."""
    qs = urlencode({"symbol": symbol})
    url = f"{BINANCE_FAPI}/fapi/v1/openInterest?{qs}"
    req = Request(url, headers={"User-Agent": "ohlcv-feed/1.0"})
    with urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    return OpenInterest(
        symbol=data["symbol"],
        open_interest=float(data["openInterest"]),
        time_ms=int(data["time"]),
    )


@dataclass(frozen=True)
class OpenInterestHist:
    symbol: str
    sum_open_interest: float
    sum_open_interest_value: float
    timestamp_ms: int


def fetch_open_interest_hist(
    symbol: str, period: str = "1h", limit: int = 48
) -> List[OpenInterestHist]:
    """Fetch historical open interest statistics from Binance Futures API.

    Period options: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    Only last 30 days data available.
    """
    qs = urlencode({"symbol": symbol, "period": period, "limit": limit})
    url = f"{BINANCE_FAPI}/futures/data/openInterestHist?{qs}"
    req = Request(url, headers={"User-Agent": "ohlcv-feed/1.0"})
    with urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read())
    result: List[OpenInterestHist] = []
    for row in payload:
        result.append(
            OpenInterestHist(
                symbol=symbol,
                sum_open_interest=float(row["sumOpenInterest"]),
                sum_open_interest_value=float(row["sumOpenInterestValue"]),
                timestamp_ms=int(row["timestamp"]),
            )
        )
    return result


def open_interest_hist_to_dataframe(oi_list: List[OpenInterestHist]) -> pd.DataFrame:
    """Convert OpenInterestHist list to DataFrame."""
    if not oi_list:
        return pd.DataFrame(
            columns=["timestamp", "sum_open_interest", "sum_open_interest_value"]
        ).astype(
            {
                "timestamp": "datetime64[ns]",
                "sum_open_interest": float,
                "sum_open_interest_value": float,
            }
        )
    df = pd.DataFrame(
        [
            {
                "timestamp": pd.to_datetime(o.timestamp_ms, unit="ms", utc=True).tz_convert(None),
                "sum_open_interest": o.sum_open_interest,
                "sum_open_interest_value": o.sum_open_interest_value,
            }
            for o in oi_list
        ]
    )
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    return df


# -----------------------------------------------------------------------------
# Long/Short Ratio (Global Account Ratio)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class LongShortRatio:
    symbol: str
    long_short_ratio: float
    long_account: float
    short_account: float
    timestamp_ms: int


def fetch_long_short_ratio(
    symbol: str, period: str = "1h", limit: int = 48
) -> List[LongShortRatio]:
    """Fetch global long/short account ratio from Binance Futures API.

    Period options: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    Only last 30 days data available.
    """
    qs = urlencode({"symbol": symbol, "period": period, "limit": limit})
    url = f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio?{qs}"
    req = Request(url, headers={"User-Agent": "ohlcv-feed/1.0"})
    with urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read())
    result: List[LongShortRatio] = []
    for row in payload:
        result.append(
            LongShortRatio(
                symbol=symbol,
                long_short_ratio=float(row["longShortRatio"]),
                long_account=float(row["longAccount"]),
                short_account=float(row["shortAccount"]),
                timestamp_ms=int(row["timestamp"]),
            )
        )
    return result


def long_short_ratio_to_dataframe(ratios: List[LongShortRatio]) -> pd.DataFrame:
    """Convert LongShortRatio list to DataFrame."""
    if not ratios:
        return pd.DataFrame(
            columns=["timestamp", "long_short_ratio", "long_account", "short_account"]
        ).astype(
            {
                "timestamp": "datetime64[ns]",
                "long_short_ratio": float,
                "long_account": float,
                "short_account": float,
            }
        )
    df = pd.DataFrame(
        [
            {
                "timestamp": pd.to_datetime(r.timestamp_ms, unit="ms", utc=True).tz_convert(None),
                "long_short_ratio": r.long_short_ratio,
                "long_account": r.long_account,
                "short_account": r.short_account,
            }
            for r in ratios
        ]
    )
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    return df


# -----------------------------------------------------------------------------
# Premium Index Klines
# -----------------------------------------------------------------------------


def fetch_premium_index_klines(symbol: str, interval: str, limit: int) -> List[Kline]:
    """Fetch premium index klines from Binance Futures API.

    Returns same Kline structure (volume will be 0 for premium index).
    """
    qs = urlencode({"symbol": symbol, "interval": interval, "limit": limit})
    url = f"{BINANCE_FAPI}/fapi/v1/premiumIndexKlines?{qs}"
    req = Request(url, headers={"User-Agent": "ohlcv-feed/1.0"})
    with urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read())
    klines: List[Kline] = []
    for row in payload:
        klines.append(
            Kline(
                open_time_ms=int(row[0]),
                open=str(row[1]),
                high=str(row[2]),
                low=str(row[3]),
                close=str(row[4]),
                volume="0",  # Premium index has no volume
                close_time_ms=int(row[6]),
            )
        )
    return klines


# -----------------------------------------------------------------------------
# Spot Klines
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class SpotKline:
    open_time_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time_ms: int
    num_trades: int
    taker_buy_base_volume: str


def fetch_spot_klines(symbol: str, interval: str, limit: int) -> List[SpotKline]:
    """Fetch spot klines from Binance Spot API.

    Includes volume, number of trades, and taker buy base volume.
    """
    qs = urlencode({"symbol": symbol, "interval": interval, "limit": limit})
    url = f"{BINANCE_SPOT_API}/api/v3/klines?{qs}"
    req = Request(url, headers={"User-Agent": "ohlcv-feed/1.0"})
    with urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read())
    klines: List[SpotKline] = []
    for row in payload:
        klines.append(
            SpotKline(
                open_time_ms=int(row[0]),
                open=str(row[1]),
                high=str(row[2]),
                low=str(row[3]),
                close=str(row[4]),
                volume=str(row[5]),
                close_time_ms=int(row[6]),
                num_trades=int(row[8]),
                taker_buy_base_volume=str(row[9]),
            )
        )
    return klines


def spot_klines_to_dataframe(klines: List[SpotKline]) -> pd.DataFrame:
    """Convert SpotKline list to DataFrame with num_trades and taker_buy_base_volume."""
    if not klines:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "num_trades",
                "taker_buy_base_volume",
                "_close_time",
            ]
        ).astype(
            {
                "timestamp": "datetime64[ns]",
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
                "num_trades": int,
                "taker_buy_base_volume": float,
            }
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
                "num_trades": k.num_trades,
                "taker_buy_base_volume": float(k.taker_buy_base_volume),
                "_close_time": pd.to_datetime(k.close_time_ms, unit="ms", utc=True).tz_convert(None),
            }
            for k in klines
        ]
    )
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    return df
