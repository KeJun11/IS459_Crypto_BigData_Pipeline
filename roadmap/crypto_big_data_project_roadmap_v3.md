# Crypto Big Data Project Roadmap v3

## Architecture Overview

### Streaming Pipeline
```
Binance WebSocket
→ Kafka (async producer, Docker on EC2)
→ Spark Structured Streaming (clean + deduplicate only)
→ ClickHouse (ReplacingMergeTree + Materialized Views)
→ Grafana
```

### Batch Pipeline
```
Binance REST API (with rate-limit handling)
→ S3 (raw Parquet)
→ Airflow (orchestration)
→ EMR Serverless (Spark batch jobs)
→ ClickHouse aggregate tables
→ Grafana
```

### Scope
- **Symbols:** BTCUSDT, ETHUSDT, SOLUSDT (3 pairs)
- **Granularity:** 1-minute OHLCV
- **Derived features:** returns, rolling averages, rolling volatility (computed in ClickHouse Materialized Views)
- **Infra:** Single EC2 t3.xlarge for streaming stack; S3 + EMR Serverless for batch

---

## Phase 0: Infrastructure Setup

**Goal:** Get environment ready before writing any pipeline code.

Tasks:
- Provision EC2 t3.xlarge on AWS (16GB RAM — see memory constraints below)
- Set up Docker Compose on EC2 with: Kafka, Spark, ClickHouse, Airflow, Grafana
- **Docker memory limits:** Set hard limits in `docker-compose.yml` for each service to prevent the Linux OOM killer from taking down services. Suggested allocation:

| Service | Memory Limit |
|---|---|
| Kafka + Zookeeper | 2GB |
| Spark (driver + executor) | 4GB total |
| ClickHouse | 4GB |
| Airflow (LocalExecutor) | 2GB |
| Grafana | 512MB |

- **Spark config:** Set `spark.driver.memory=1g` and `spark.executor.memory=2g` — the streaming job is lightweight (clean + dedup only), so this is sufficient
- **Swap space:** Enable a 4–8GB swap file on the EC2 instance as a safety net against memory spikes
- **Use Airflow with `LocalExecutor`** (not CeleryExecutor) — significantly lower RAM footprint for a single-machine setup
- Create S3 bucket: `s3://your-project/raw/`
- Set up IAM roles for EC2 → S3 and EMR → S3 access
- Document schemas upfront in a `schemas/` folder (see Phase 2)

> **Local machine note:** The same RAM constraints apply if you're developing locally. Apply the same Docker memory limits and swap space on your laptop. t3.xlarge is recommended for the demo/final run due to stability.

---

## Phase 1: Historical Data Ingestion

**Goal:** Build a backfill dataset in S3 using the Binance REST API.

Tasks:
- Write a Python script to pull 1-minute candles via `GET /api/v3/klines`
- **Rate limit handling:** Binance allows 1200 request weight/min; each klines call costs 2 weight. Read the `X-MBN-USED-WEIGHT` response header and back off dynamically when approaching the limit — don't just use a fixed `time.sleep()`
- Pull ~3 months of history per symbol
- Save as partitioned Parquet to S3: `s3://bucket/raw/symbol=BTCUSDT/year=.../month=.../`
- Sanity check: row counts, date coverage, null values

> **Note on Kaggle:** You can use a Kaggle dataset to bootstrap ClickHouse and Spark testing in early phases before your API pull finishes. Replace it with real API data before final submission.

---

## Phase 2: Schema and Data Contract Design

**Goal:** Lock in data contracts before building processing logic.

Kafka message format (JSON):
```json
{ "symbol": "BTCUSDT", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
  "volume": 100.0, "open_time": 1700000000000, "close_time": 1700000059999, "is_closed": true }
```

ClickHouse tables:

| Table | Engine | Description |
|---|---|---|
| `raw_ohlcv_1m` | **ReplacingMergeTree** | Raw 1-min candles — stream + batch both write here |
| `agg_ohlcv_1h` | ReplacingMergeTree | Hourly aggregates |
| `agg_ohlcv_1d` | ReplacingMergeTree | Daily aggregates |
| `technical_features` | Materialized View | Rolling MAs, volatility, returns — auto-computed |
| `anomaly_events` | MergeTree | Flagged outliers |

**Why `ReplacingMergeTree` everywhere:** Both the WebSocket stream and the REST batch pipeline write to the same tables. The REST API provides the "official" final candle values; `ReplacingMergeTree` ensures that if the same `(symbol, open_time)` arrives twice, the latest version wins — no duplicates, no conflicts between live and historical data.

All tables: partition by `toYYYYMM(open_time)`, order by `(symbol, open_time)`.

---

## Phase 3: Real-Time Ingestion

**Goal:** Stream live candle data from Binance into Kafka.

Tasks:
- Subscribe to `<symbol>@kline_1m` WebSocket streams for all 3 symbols
- Use an **async Kafka producer** (e.g. `aiokafka`) — do not use a synchronous producer. A sync producer blocks the WebSocket event loop waiting for Kafka acknowledgement, which causes stream lag and potential message loss
- Push each closed candle (`is_closed: true`) to Kafka topic `crypto_ohlcv_1m`
- Use a consumer group so Spark and any other consumers read independently
- Kafka retention: 24–48 hours (enough for replay and debugging)

> **Kafka Connect note:** For documentation purposes, mention that in a production system you would use Kafka Connect with a custom source connector instead of a Python script. It shows architectural awareness without adding scope.

---

## Phase 4: Stream Processing with Spark

**Goal:** Consume Kafka stream, clean and deduplicate, write to ClickHouse.

**Scope discipline:** Keep this job focused on cleaning only. Do not compute rolling analytics here — ClickHouse Materialized Views handle that faster and with less complexity (see Phase 2 schema).

Tasks:
- Read from `crypto_ohlcv_1m` using Spark Structured Streaming
- Parse and validate JSON schema; drop malformed rows
- Deduplicate by `(symbol, open_time)` using a watermark of 5 minutes to handle slightly late messages
- Write cleaned rows to ClickHouse `raw_ohlcv_1m`
- Configure **checkpointing to S3** (`s3://bucket/checkpoints/`) — allows the stream to resume after a restart without data loss or reprocessing

---

## Phase 5: Analytical Layer — ClickHouse Materialized Views

**Goal:** Compute rolling analytics inside ClickHouse rather than in Spark.

Why here instead of Spark: ClickHouse window functions are extremely fast over time-series data, execute at query time or incrementally on insert, and keep the Spark job lightweight.

Materialized Views to create:
- **Moving averages:** 5-period and 15-period MA on `close`, auto-updated on each insert into `raw_ohlcv_1m`
- **Rolling volatility:** rolling standard deviation of returns over a configurable window
- **Returns:** `(close - prev_close) / prev_close` per symbol

These power the Grafana dashboard directly — no extra processing step needed.

---

## Phase 6: Batch Processing with EMR

**Goal:** Use EMR Serverless for scheduled Spark batch jobs over historical S3 data.

Tasks:
- Write Spark batch jobs for:
  - Hourly and daily aggregate generation → write to `agg_ohlcv_1h`, `agg_ohlcv_1d`
  - Data quality checks (gap detection, duplicate counts, range validation)
  - Feature recomputation over historical windows when schema changes
- Submit jobs as EMR Serverless runs (pay per vCPU-second, no idle cluster cost)
- Read from S3 Parquet; write back to ClickHouse

---

## Phase 7: Orchestration with Airflow

**Goal:** Schedule and monitor all batch workflows.

Run Airflow with `LocalExecutor` on EC2 Docker — avoids the RAM cost of CeleryExecutor's worker processes.

DAGs to build:
- **Daily aggregate refresh:** trigger EMR job → validate row counts → alert on failure
- **Weekly data quality check:** scan `raw_ohlcv_1m` for gaps, duplicates, out-of-range values
- **Backfill DAG:** parameterized by date range, for on-demand reprocessing

Airflow features to use:
- **S3KeySensor** to wait for Parquet files before triggering EMR jobs
- **XComs** to pass row counts or validation results between tasks
- **Retries with exponential backoff** on all EMR and API tasks

---

## Phase 8: Data Quality Layer

**Goal:** Make the pipeline trustworthy, not just functional.

Checks (live inside Airflow DAGs or as a standalone Spark job):
- **Gap detection:** find missing 1-minute intervals per symbol
- **Duplicate detection:** verify `(symbol, open_time)` uniqueness after dedup
- **Range validation:** flag candles where `low > high` or `volume = 0` unexpectedly
- **Freshness check:** alert if latest `open_time` in ClickHouse is more than 5 minutes old (pipeline stalled)

---

## Phase 9: Dashboard with Grafana

**Goal:** Visualise pipeline outputs cleanly for the demo.

Setup:
- Install the **official ClickHouse data source plugin** for Grafana — 2 minutes to configure, direct SQL queries
- Point panels at ClickHouse Materialized Views, not raw tables, for performance

Panels to build:
- Live 1-minute price chart per symbol
- Moving average overlays (from Materialized Views)
- Rolling volatility
- Hourly volume bars
- **Pipeline health panel:** latest ingestion timestamp + gap count — demonstrates the pipeline is live, not just the charts

---

## AWS Cost Guide ($300 credits)

| Service | Usage | Estimated Cost |
|---|---|---|
| EC2 t3.xlarge | ~10 hrs/week × 12 weeks | ~$20 |
| S3 | ~10GB raw + aggregates | ~$2–3/month |
| EMR Serverless | ~5 jobs/week, ~10 min each | ~$5–15/month |
| Data transfer | Minimal at this scale | ~$2–5 |
| **Total estimate** | | **~$50–80 for the semester** |

**Do not use:**
- MSK (Managed Kafka) — ~$0.21/hr per broker minimum
- MWAA (Managed Airflow) — ~$0.49/hr just for the environment baseline

---

## Final Stack

| Layer | Tool | Notes |
|---|---|---|
| Data source | Binance REST + WebSocket | One source, both patterns |
| Message bus | Kafka (Docker on EC2) | Async producer |
| Stream processing | Spark Structured Streaming | Clean + dedup only |
| Analytics | ClickHouse Materialized Views | Rolling windows, MAs |
| Batch processing | Spark on EMR Serverless | Pay-per-run |
| Raw storage | S3 (Parquet, partitioned) | Source of truth |
| Analytical storage | ClickHouse (ReplacingMergeTree) | Handles stream + batch upserts |
| Orchestration | Airflow LocalExecutor (Docker) | Low RAM footprint |
| Dashboarding | Grafana + ClickHouse plugin | Native time-series support |

---

## Optional Extensions (after core pipeline works)
- Anomaly detection on price/volume spikes → `anomaly_events` table
- Simple ML feature store for forecasting experiments
- Add more symbols or a second exchange for cross-venue comparison
- Pipeline latency monitoring (measure WebSocket → ClickHouse end-to-end lag)
