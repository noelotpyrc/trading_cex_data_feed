# Testing Guide

## Running Tests

### All tests (offline)

```bash
python -m pytest tests/
```

### Binance module tests

```bash
# API tests
python tests/binance/test_api.py

# Database tests
python tests/binance/test_db.py

# Validation tests
python tests/binance/test_validation.py

# Persistence tests
python tests/binance/test_persistence.py

# CLI tests (dry-run)
python tests/binance/test_cli.py

# CLI catch-up tests
python tests/binance/test_cli_catchup.py
```

### E2E tests (requires live API)

```bash
# Requires internet connection to Binance API
python tests/binance/test_api_e2e.py --run

# Compare API vs CSV data
python tests/binance/test_compare_api_vs_csv.py
```

## Test Structure

```
tests/
├── binance/              # Binance-specific tests
│   ├── test_api.py       # API client unit tests
│   ├── test_db.py        # DuckDB operations tests
│   ├── test_validation.py # Validation logic tests
│   ├── test_persistence.py # Snapshot persistence tests
│   ├── test_cli.py       # CLI dry-run tests
│   ├── test_cli_catchup.py # Multi-row catch-up tests
│   ├── test_api_e2e.py   # Live API E2E test
│   └── test_compare_api_vs_csv.py # Data consistency tests
└── README.md             # This file
```

## Test Coverage

The test suite covers:
- API client behavior
- DuckDB schema and operations
- Data validation logic
- CLI argument parsing and dry-run mode
- Catch-up mode for multiple missing rows
- Live API interaction (optional E2E tests)

## Continuous Integration

To run tests in CI, ensure:
1. Python environment with dependencies installed
2. Skip E2E tests unless explicitly enabled
3. Use temporary directories for test databases

Example CI command:
```bash
pytest tests/ -v --ignore=tests/binance/test_api_e2e.py
```
