"""CEX Data Feed - Data ingestion utilities for cryptocurrency exchanges.

Provides:
- Binance OHLCV feed (real-time and historical)
- CLI scripts for downloading and merging klines
- DuckDB persistence layer
"""

__version__ = "0.1.0"

# Expose main submodules
from . import binance
from . import scripts

__all__ = ["binance", "scripts", "__version__"]
