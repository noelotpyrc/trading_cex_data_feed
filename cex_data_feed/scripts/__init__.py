"""CLI scripts for downloading and processing exchange data.

Scripts:
- download_binance_monthly_klines: Download historical monthly klines from Binance Vision
- download_binance_daily_klines: Download recent daily klines from Binance Vision
- merge_binance_klines: Merge multiple kline CSV files into a single dataset
- backfill_ohlcv_binance_1h_from_csv: Backfill DuckDB from CSV file
- backfill_recent_from_api: Backfill recent hours from Binance API
- export_duckdb_ohlcv_to_csv: Export DuckDB OHLCV data to CSV

Usage:
    python -m cex_data_feed.scripts.download_binance_monthly_klines --help
    python -m cex_data_feed.scripts.backfill_ohlcv_binance_1h_from_csv --help
"""

__all__ = [
    "download_binance_monthly_klines",
    "download_binance_daily_klines",
    "merge_binance_klines",
    "backfill_ohlcv_binance_1h_from_csv",
    "backfill_recent_from_api",
    "export_duckdb_ohlcv_to_csv",
]
