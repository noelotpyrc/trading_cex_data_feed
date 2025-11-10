# CEX Data Feed

Data ingestion utilities for cryptocurrency exchanges (CEX). Provides real-time and historical OHLCV data feeds with DuckDB persistence.

## Features

- **Binance Feed**: Semi real-time feed and backfill utilities for Binance USDT-M Futures
- **Data Utilities**: Download and merge historical klines from Binance Vision
- **DuckDB Storage**: Minimal append-only store with validation
- **Leakage Prevention**: Closed-candle filtering to avoid look-ahead bias

## Setup

```bash
cd /Users/noel/projects/trading_cex_data_feed
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### 1. Download Historical Data

Download monthly historical data from Binance Vision:

```bash
python -m cex_data_feed.scripts.download_binance_monthly_klines \
  --symbol BTCUSDT \
  --interval 1h \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --output-dir ./data/binance_monthly
```

Merge the downloaded files:

```bash
python -m cex_data_feed.scripts.merge_binance_klines \
  --in-dir ./data/binance_monthly \
  --out-file ./data/BTCUSDT_1h_merged.csv
```

### 2. Backfill DuckDB from CSV

```bash
python -m cex_data_feed.scripts.backfill_ohlcv_binance_1h_from_csv \
  --csv ./data/BTCUSDT_1h_merged.csv \
  --duckdb ./data/ohlcv.duckdb \
  --stop-on-gap
```

### 3. Real-time Feed (Hourly)

Run once per hour to append the latest closed candle:

```bash
python -m cex_data_feed.binance.cli \
  --duckdb ./data/ohlcv.duckdb \
  --persist-dir ./data/feed_artifacts \
  --dataset binance_btcusdt_perp_1h \
  --n-recent 12
```

Add `--catch-up` flag to backfill multiple missing hours.

### 4. Export to CSV

```bash
python -m cex_data_feed.scripts.export_duckdb_ohlcv_to_csv \
  --duckdb ./data/ohlcv.duckdb \
  --output ./data/exported_ohlcv.csv
```

### 5. Backfill DuckDB from Binance API

```bash
python -m cex_data_feed.scripts.backfill_recent_from_api --duckdb "/Volumes/Extreme SSD/trading_data/cex/db/binance_btcusdt_perp_ohlcv.duckdb" --n-recent 120 --db-validate-rows 144
```

## Module Structure

```
cex_data_feed/
├── binance/              # Binance-specific feed implementation
│   ├── api.py            # API client for fetching klines
│   ├── db.py             # DuckDB operations
│   ├── validation.py     # Data validation utilities
│   ├── persistence.py    # Snapshot persistence
│   └── cli.py            # Hourly feed CLI
└── scripts/              # CLI scripts for data download and processing
    ├── download_binance_monthly_klines.py
    ├── download_binance_daily_klines.py
    ├── merge_binance_klines.py
    ├── backfill_ohlcv_binance_1h_from_csv.py
    ├── backfill_recent_from_api.py
    └── export_duckdb_ohlcv_to_csv.py
```

## Documentation

- [Binance Feed Details](docs/binance_feed.md) - Full API reference and workflows
- [Testing Guide](tests/README.md) - How to run tests

## Testing

```bash
# Run all tests
python -m pytest tests/

# Specific test files
python tests/binance/test_api.py
python tests/binance/test_db.py
python tests/binance/test_validation.py

# E2E test (requires live API)
python tests/binance/test_api_e2e.py --run
```

## Data Conventions

- **Timestamps**: UTC-naive `TIMESTAMP` in DuckDB
- **Closed candles only**: A candle is considered closed if `close_time <= now_utc.floor('h') - 1ms`
- **Append-only**: Inserts are guarded by unique timestamp index
