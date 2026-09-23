"""Public REST adapters. Network transport is injectable for deterministic tests."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from .contract import Observation, MINUTE, SPECS


def now_ms():
    return time.time_ns() // 1_000_000


class DeferredRequest(RuntimeError):
    def __init__(self, message, retry_at_ms):
        super().__init__(message)
        self.retry_at_ms = retry_at_ms


def get_json(url, *, attempts=3, timeout=10, sleeper=time.sleep, opener=urlopen):
    for attempt in range(attempts):
        try:
            with opener(Request(url, headers={"User-Agent": "cex-rv-live/0.1"}), timeout=timeout) as response:
                body = response.read(4 * 1024 * 1024 + 1)
                if len(body) > 4 * 1024 * 1024:
                    raise ValueError("Response exceeds 4MiB")
                return json.loads(body), now_ms()
        except (HTTPError, URLError, TimeoutError) as error:
            if isinstance(error, HTTPError) and error.code not in (418, 429, 500, 502, 503, 504):
                raise
            delay = 2 ** attempt
            if isinstance(error, HTTPError) and error.headers.get("Retry-After"):
                value = error.headers["Retry-After"]
                try:
                    delay = max(delay, float(value))
                except ValueError:
                    delay = max(delay, parsedate_to_datetime(value).timestamp() - time.time())
            # A long venue backoff is surfaced to the scheduler, never shortened into aggressive retries.
            if attempt + 1 == attempts or delay > 30:
                raise DeferredRequest(str(error), now_ms() + int(max(delay, 1) * 1000)) from error
            sleeper(max(0, delay))
    raise RuntimeError("No request attempts")


def request_url(source, start, end):
    """Request one bounded [start,end) page; caller handles pagination."""
    if source in ("binance", "mark", "premium", "funding"):
        endpoint = {"binance": "klines", "mark": "markPriceKlines",
                    "premium": "premiumIndexKlines", "funding": "fundingRate"}[source]
        args = dict(symbol="BTCUSDT", startTime=start, endTime=end - 1, limit=200)
        if source != "funding":
            args["interval"] = "1m"
        return "https://fapi.binance.com/fapi/v1/" + endpoint + "?" + urlencode(args)
    if source == "coinbase":
        def iso(t):
            return datetime.fromtimestamp(t / 1000, timezone.utc).isoformat()
        return "https://api.exchange.coinbase.com/products/BTC-USD/candles?" + urlencode(
            dict(granularity=300, start=iso(start), end=iso(end)))
    if source == "deribit":
        return "https://www.deribit.com/api/v2/public/get_tradingview_chart_data?" + urlencode(
            dict(instrument_name="BTC-PERPETUAL", resolution="5", start_timestamp=start, end_timestamp=end - 1))
    raise ValueError("Unknown source")


def parse(source, payload, received_ms, start, end):
    rows = []
    if source in ("binance", "mark", "premium"):
        if not isinstance(payload, list):
            raise ValueError(f"API error: {payload}")
        for r in payload:
            t = int(r[0])
            if int(r[6]) != t + MINUTE - 1:
                raise ValueError("Unexpected Binance candle close time")
            v = dict(zip(("open", "high", "low", "close"), map(float, r[1:5])))
            if source == "binance":
                v.update(volume=float(r[5]), quote_asset_volume=float(r[7]), num_trades=int(r[8]),
                         taker_buy_base_volume=float(r[9]), taker_buy_quote_volume=float(r[10]))
            rows.append((t, v))
    elif source == "funding":
        if not isinstance(payload, list):
            raise ValueError(f"API error: {payload}")
        rows = [(int(r["fundingTime"]), {"rate": float(r["fundingRate"])}) for r in payload]
    elif source == "coinbase":
        if not isinstance(payload, list):
            raise ValueError(f"API error: {payload}")
        rows = [(int(r[0]) * 1000, dict(low=float(r[1]), high=float(r[2]), open=float(r[3]),
                                      close=float(r[4]), volume=float(r[5]))) for r in payload]
    elif source == "deribit":
        if "error" in payload:
            raise ValueError(f"API error: {payload['error']}")
        result = payload["result"]
        if result["status"] == "no_data":
            return []
        if result["status"] != "ok":
            raise ValueError("Unexpected Deribit status")
        fields = ("open", "high", "low", "close", "volume")
        if any(len(result[f]) != len(result["ticks"]) for f in fields):
            raise ValueError("Unequal Deribit array lengths")
        rows = [(int(t), {f: float(result[f][i]) for f in fields}) for i, t in enumerate(result["ticks"])]
    else:
        raise ValueError("Unknown source")
    interval = SPECS[source][0]
    out = [Observation(source, t, v, received_ms).validate() for t, v in rows
           if start <= t < end and t + interval <= received_ms]
    by_time = {}
    for row in out:
        if row.t in by_time and by_time[row.t].fingerprint != row.fingerprint:
            raise ValueError("Conflicting duplicate timestamps within response")
        by_time[row.t] = row
    return sorted(by_time.values(), key=lambda r: r.t)


class Adapter:
    def __init__(self, source, transport=get_json):
        if source not in SPECS:
            raise ValueError("Unknown source")
        self.source, self.transport = source, transport

    def fetch(self, start, end):
        url = request_url(self.source, start, end)
        payload, received = self.transport(url)
        return parse(self.source, payload, received, start, end), payload, received, url
