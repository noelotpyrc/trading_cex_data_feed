"""Canonical observation contract. All timestamps are integer UTC milliseconds."""
from dataclasses import dataclass
import hashlib
import json
import math

MINUTE = 60_000
SPECS = {
    "binance": (MINUTE, "BTCUSDT"),
    "mark": (MINUTE, "BTCUSDT"),
    "premium": (MINUTE, "BTCUSDT"),
    "funding": (0, "BTCUSDT"),
    "coinbase": (5 * MINUTE, "BTC-USD"),
    "deribit": (5 * MINUTE, "BTC-PERPETUAL"),
}
ACTIVITY = ("volume", "quote_asset_volume", "num_trades", "taker_buy_base_volume",
            "taker_buy_quote_volume")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def release_ms(source, t):
    if source == "binance":
        return t + MINUTE
    if source in ("mark", "premium"):
        return t + 119_999
    if source == "funding":
        return t + MINUTE
    return t + 6 * MINUTE


def expiry_ms(source, t):
    age = {"mark": MINUTE, "premium": MINUTE, "funding": 480 * MINUTE,
           "coinbase": 5 * MINUTE, "deribit": 5 * MINUTE}
    return release_ms(source, t) + age[source]


@dataclass(frozen=True)
class Observation:
    source: str
    t: int
    values: dict
    received_ms: int

    def validate(self):
        if self.source not in SPECS:
            raise ValueError("Unknown source")
        if type(self.t) is not int or type(self.received_ms) is not int or self.t < 0:
            raise ValueError("Timestamps must be nonnegative integer milliseconds")
        interval, _ = SPECS[self.source]
        if interval and self.t % interval:
            raise ValueError("Bar is not UTC interval-aligned")
        if self.received_ms < self.t + interval:
            raise ValueError("Forming/future observation")
        required = {"rate"} if self.source == "funding" else {"open", "high", "low", "close"}
        if self.source == "binance":
            required.update(ACTIVITY)
        if self.source in ("coinbase", "deribit"):
            required.add("volume")
        if set(self.values) != required:
            raise ValueError(f"Wrong fields for {self.source}: {set(self.values) ^ required}")
        if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
               for v in self.values.values()):
            raise ValueError("All market values must be finite numbers")
        v = self.values
        if interval:
            if v["high"] < max(v["open"], v["close"], v["low"]) or v["low"] > min(v["open"], v["close"]):
                raise ValueError("Invalid candle envelope")
            if self.source != "premium" and min(v[k] for k in ("open", "high", "low", "close")) <= 0:
                raise ValueError("Nonpositive price")
        for key in ACTIVITY:
            if key in v and v[key] < 0:
                raise ValueError("Negative activity")
        if self.source == "binance":
            if v["num_trades"] != int(v["num_trades"]):
                raise ValueError("Nonintegral trade count")
            if v["taker_buy_base_volume"] > v["volume"] + 1e-12 or v["taker_buy_quote_volume"] > v["quote_asset_volume"] + 1e-8:
                raise ValueError("Taker volume exceeds total")
        return self

    @property
    def fingerprint(self):
        return digest({"source": self.source, "t": self.t, "values": self.values})
