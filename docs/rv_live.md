# RV × EMA public-data pipeline

## Scope and status

The new `cex_data_feed.rv_live` package collects and versions the six inputs required
by the existing BTCUSDT RV1440 model, then evaluates q75/q90 regimes and EMA_DIST1440
bottom-10% long / top-5% short conditions. It emits signal states, not orders.
Existing collectors, databases and configuration are unchanged.

This first version uses completed-bar REST polling. Binance trade, mark and premium
bars are collected every minute; Coinbase/Deribit bar windows advance every five
minutes and settled funding collection advances every fifteen minutes. Incomplete
windows are retried. WebSocket acquisition is a future latency optimization. The
historical generator's prescribed release times are not measured live-feed latency.

Offline validation has passed. Endpoint connectivity and prospective availability
remain unverified; the service has not been activated. Current saved EMA thresholds
end in July 2026, so that exported bundle cannot generate September 2026 live fires.
Missing or expired artifacts produce an unavailable decision.

## Components

| Module | Responsibility |
|---|---|
| `adapters.py` | Public REST requests, bounded retry/backoff, six source parsers |
| `contract.py` | UTC milliseconds, units/field checks, causal release/expiry rules |
| `store.py` | WAL SQLite, receipt history, source revisions, immutable decisions, atomic checkpoints |
| `collect.py` | Timestamp-range pagination, explicit gap reports, persistent repair cursors/backoff |
| `signals.py` | Exact EMA state, original 40-feature recipe, frozen model/threshold inference |
| `historical_features.py` | Unmodified historical feature builder, protected by a source checksum |
| `build_bundle.py` | Export saved models/calibrations and exact historical EMA state; no fitting |
| `validate.py` | Strict decision comparison and first-seen/final observation reconciliation |

Each bar is keyed by source and UTC **opening** time. Coinbase/Deribit volumes are
base BTC volumes from their candle endpoints. Premium OHLC may be negative. All
required Binance activity fields must be finite; no missing-bar interpolation occurs.

SQLite records each normalized change with its receipt time and content hash. Raw
successful REST responses are also retained. An identical re-fetch does not create
a new normalized version. Revisions never overwrite previous decisions.

## Correctness: three separate comparisons

### A. Identical-input historical replay

Feed historical observations to the new engine in causal order, using the historical
release schedule. Load the same model, threshold versions and EMA bootstrap. Compare
against independently saved historical features/predictions and batch EMA calculations.

Required checks, by decision minute:

- All 40 features: `rtol=atol=1e-10`.
- EMA and EMA distance: `rtol=atol=1e-10`.
- RV score: `rtol=atol=1e-12` (the observed replay scores were exactly equal).
- Regime, low/high fire flags, threshold values, eligibility and timestamps: exact.
- Missing/extra decision rows fail; unavailable rows are not silently excluded.

A small numerical tolerance never excuses a changed fire flag. Signals near a
threshold must retain the same comparison operators: RV `>`, EMA low `<=`, EMA high `>`.

RLF-003 replay covered 600 minutes / 40 prediction boundaries in fixed windows
2025-07-15 00:00–06:00 UTC and 2025-07-31 22:00–2025-08-01 02:00 UTC.
It included checkpoint restarts and a monthly threshold change. Zero categorical
mismatches; score error 0; maximum feature error 1.85e-15; EMA-distance error 1.65e-13.
These windows exercised q75 low fires; q90 high and upper-tail fires were absent
in these slices and require synthetic boundary tests plus prospective coverage.

### B. Replay what the live process actually knew

For every live decision save logical minute, actual emission time, receipt cutoff,
model/bundle identities, input hashes, score, EMA distance, threshold values and flags.
Reconstruct inputs from the versioned store **as of that receipt cutoff**, respecting
the prescribed releases. Replaying those observations with the same checkpoint and
artifacts must reproduce the original result. Use a separate run ID for any replay.

The default decision deadline is minute +30 seconds. A delayed observation cannot
become known at an earlier decision time just because it is in the database now.
An unavailable decision remains immutable. Missed history can update later state,
but recovered past decisions must be explicitly marked replay, never fresh fires.

### C. Reconcile with finalized historical inputs

Later, collect the same timestamps again. Compare first-seen and finalized versions,
then run the historical generator on the finalized inputs. Attribute differing fires
to late/missing data, revisions, artifact differences, or a calculation error.

Example: at 13:15 the historical recipe expects Binance trade bars through 13:14,
mark/premium through 13:13, and cross-venue 13:05–13:10 bars. If a required observation
arrives at 13:15:40, the +30-second live deadline excludes it. Historical replay may
show a fire; the correct live result is unavailable. That is an availability difference.

Conversely, matching inputs/artifacts with a different fire is a correctness failure.
Compare every minute, including no-fire/unavailable rows, rather than only fired rows
or total PnL. A bounded shadow period should report missing inputs, receipt/emission
latency, missed/extra fire minutes and counts of each mismatch cause.

## Setup

Use an isolated Python 3.11 environment for this pipeline. `requirements-rv-live.txt`
pins the versions used by the saved models; it need not replace the repository's
shared production environment. Public HTTP uses the standard library.

```bash
python -m pip install -r requirements-rv-live.txt
python -m pytest -q tests/rv_live
python -m cex_data_feed.rv_live --help
```

Use a **new** database, e.g. `data/rv_live.sqlite`. The package refuses to initialize
inside an existing legacy database. Source updates are read-only public API requests;
no exchange credentials are accepted.

## Bootstrap and artifact export

`build_bundle` accepts the research project explicitly. It reads only the named saved
model/calibration/raw-history artifacts, never targets or PnL, and performs no fit.
The output directory must be new. Choose `bootstrap-through-ms` immediately before
the contiguous trade-bar history that the consumer will process into its first decision.
For example, preserve at least 10,081 raw minutes before that first decision and seed
the EMA at the preceding minute. The seed must include the full EMA history since
January 2020; restarting the EMA from the warmup window changes the signal.

```bash
python -m cex_data_feed.rv_live.build_bundle \
  --research-root /Users/noel/projects/trading_strategy_exploration \
  --output /tmp/rv_bundle \
  --bootstrap-through-ms EPOCH_MS
```

Bundle manifest contains ordered features; exact EMA seed/provenance; quarterly
model checksums, validity and RV cutoffs; monthly EMA cutoffs; source hashes.
Only load trusted local joblib files. Models must reside within the bundle directory.
This exporter packages existing calibrations; scheduling future quarterly fits and
monthly recalibration is an offline artifact-production task, not automatic tuning
by the live service. Do not run with expired artifacts.

Bootstrap the source store with timestamp-range repair (or normalized JSONL imports).
For an actual deployment, old bars obtained now retain their real current receipt
times: do not backdate their receipt metadata to manufacture a live history.
Historical replay fixtures can use prescribed synthetic receipts but must be labelled
as replay and kept in a separate database/run.

```bash
python -m cex_data_feed.rv_live repair --db data/rv_live.sqlite \
  --source binance --start-ms START_MS --end-ms END_MS --max-pages 100
```

Repeat for `mark`, `premium`, `funding`, `coinbase`, `deribit`. Bounds are exclusive
on the right and must align with the source interval. Funding events need at least
481 minutes of coverage before a prediction; cross-venue returns need adjacent bars.
Check the completeness report; a page budget or missing minute returns nonzero.
An empty funding response is not proof of fresh funding; the consumer separately
checks the event's age.

For bulk bootstrap, `import-jsonl --db ... --input ...` accepts one object per line:
`{"source":"binance","t":<open_ms>,"received_ms":<receipt_ms>,"values":{...}}`.
Import is chunked; a failed import may have committed earlier chunks. It fails loudly
and can be resumed idempotently. It does not silently skip malformed rows.

## Run and inspect

One bounded collection pass:

```bash
python -m cex_data_feed.rv_live collect --db data/rv_live.sqlite \
  --start-ms BOOTSTRAP_START_MS --end-ms LAST_CLOSED_BOUNDARY_MS --max-pages 4
```

Continuous foreground shadow process, after bootstrap and current artifacts:

```bash
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 python -m cex_data_feed.rv_live watch \
  --db data/rv_live.sqlite --start-ms BOOTSTRAP_START_MS \
  --bundle /path/to/bundle/manifest.json --run shadow-v1
```

No service is automatically installed. The process starts work five seconds after
each minute boundary. Collection status goes to stderr; decisions go to stdout and
the database. Polling is sequential; slow requests can miss the deadline, in which
case the decision is unavailable. Use one writer/runner per database. Checkpoints
also reject competing state changes rather than silently overwrite one another.

The engine evaluates every qualifying minute, including repeated fires while a
condition stays true. RV refreshes only on 15-minute boundaries and expires at the
next boundary. `report_eligible` separately captures quarter-start warmup/quarter-end
holding constraints. It does not depend on future prices or dataset-end label maturity.
Position sizing, entry throttling and execution are outside this package.

A close-price revision to already-consumed Binance history stops further signals with
`history_revision_requires_rebootstrap`, because continuing the old EMA with revised
history would mix close-price versions. Activity or other OHLC revisions do not change EMA;
the next scheduled RV prediction uses their latest as-of versions while earlier
scores remain immutable. Rebuild a consistent bootstrap and replay under a new
run ID, retaining the old decisions. Missing raw minutes must be repaired; they are
never filled with invented candles.

## Audit commands

```bash
python -m cex_data_feed.rv_live export --db data/rv_live.sqlite --run shadow-v1 > actual.jsonl
python -m cex_data_feed.rv_live compare --expected expected.jsonl --actual actual.jsonl
python -m cex_data_feed.rv_live reconcile --db data/rv_live.sqlite \
  --source binance --start-ms START_MS --end-ms END_MS \
  --first-asof-ms INITIAL_RECEIPT_CUTOFF --final-asof-ms LATER_RECEIPT_CUTOFF
```

`evaluate` supports a single decision with explicit `--minute-ms`, `--received-by-ms`,
`--bundle`, `--run`, and `--db`; historical evaluations require `--replay` at the CLI.
Use a new run ID and consistent bootstrap when comparing alternative snapshots.

## Official source contracts consulted

- [Binance REST market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data): futures klines, mark/premium klines and funding events.
- [Coinbase product candles](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles): 300-second candles; missing intervals are possible. Its docs recommend WebSocket-based collection for ongoing real-time use; this REST implementation is the initial shadow/reconciliation version.
- [Deribit chart data](https://docs.deribit.com/api-reference/market-data/public-get_tradingview_chart_data): resolution 5; `volume` is base currency, distinct from quote `cost`.

Prospective data availability and finalized/live parity must be measured on the
intended host before treating these signals as an executable feed.
