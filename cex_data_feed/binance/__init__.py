"""Binance BTCUSDT Perp 1H OHLCV semi real-time feed.

Implements data pulling, DuckDB management, validation, and artifact persistence.
Inference CSV generation is intentionally omitted for now.
"""

__all__ = [
    "api",
    "db",
    "validation",
]

