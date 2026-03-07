# Crypto Big Data Project Roadmap v4

## Architecture Overview

### Streaming Pipeline
```
Binance WebSocket
→ Kafka (async producer, Schema Registry, Docker on EC2)
→ Spark Structured Streaming (clean + deduplicate only)
→ ClickHouse Silver: raw_ohlcv_1m (ReplacingMergeTree)
→ ClickHouse Gold: Materialized Views (aggregates, rolling analytics)
→ Grafana (analytics dashboard + pipeline health dashboard)
```

### Batch Pipeline
```
Binance REST API (with rate-limit handling)
→ S3 Bronze: raw Parquet
→ Airflow (orchestration)
→ EMR Serverless (Spark batch jobs)
→ S3 Silver: cleaned Parquet
→ ClickHouse Gold: aggregate tables
→ Grafana
```

### Data Lake Layout — Medallion Architecture
```
s3://your-project/
├── bronze/   ← raw, unmodified: REST Parquet + Kafka message archive
├── silver/   ← cleaned, deduplicated Parquet after Spark validation
└── gold/     ← not stored in S3; served from ClickHouse aggregates
```

### Scope
- **Symbols:** BTCUSDT, ETHUSDT, SOLUSDT (3 pairs)
- **Granularity:** 1-minute OHLCV
- **Derived features:** returns, rolling MAs, rolling volatility (ClickHouse Materialized Views)
- **Infra:** Single EC2 t3.xlarge for streaming stack; S3 + EMR Serverless for batch

---

## Phase 0: Infrastructure Setup

**Goal:** Get environment ready before writing any pipeline code.

Tasks:
- Provision EC2 t3.xlarge on AWS (16GB RAM — see memory limits below)
- Set up Docker Compose on EC2 with: Kafka, Spark, ClickHouse, Airflow, Grafana, Schema Registry
- **Docker memory limits:** Set hard limits in `docker-compose.yml` to prevent OOM kills:

| Service | Memory Limit |
|---|---|
| Kafka + Zookeeper | 2GB |
| Confluent Schema Registry | 512MB |
| Spark (driver + executor) | 4GB total |
| ClickHouse | 4GB |
| Airflow (LocalExecutor) | 2GB |
| Grafana | 512MB |

- **Spark config:** `spark.driver.memory=1g`, `spark.executor.memory=2g` — sufficient for a clean+dedup-only streaming job
- **Swap space:** Enable a 4–8GB swap file on the EC2 instance as a safety net
- **Airflow:** Use `LocalExecutor` (not CeleryExecutor) — significantly lower RAM footprint
- Create S3 bucket with Bronze/Silver/Gold prefixes: `s3://your-project/bronze/`, `/silver/`
- Set up IAM roles: EC2 → S3, EMR → S3
- Document schemas in a `schemas/` folder in your repo

> **Local machine note:** Same RAM constraints apply during local development. Apply the same Docker limits and swap. Use EC2 for the final demo run for stability.

> **Two-machine option:** If you hit RAM pressure, the cleanest split is Machine 1 (Kafka + Airflow + Grafana) and Machine 2 (Spark + ClickHouse). Both t3.xlarge instances run ~$40 total for the semester at 10hrs/week.

---

## Phase 1: Historical Data Ingestion (Bronze)

**Goal:** Pull raw historical data from Binance and land it in S3 Bronze.

Tasks:
- Write a Python script to pull 1-minute candles via `GET /api/v3/klines`
- **Rate limit handling:** Binance allows 1200 request weight/min; each klines call costs 2 weight. Read the `X-MBN-USED-WEIGHT` response header and back off dynamically — don't use a fixed `time.sleep()`
- Pull ~3 months of history per symbol
- Save raw output as partitioned Parquet to S3 Bronze: `s3://bucket/bronze/symbol=BTCUSDT/year=.../month=.../`
- Sanity check: row counts, date coverage, null values

> **Note on Kaggle:** You can use a Kaggle dataset to bootstrap ClickHouse and Spark testing in early phases. Replace with real API data before final submission.

---

## Phase 2: Schema and Data Contract Design

**Goal:** Lock in data contracts before building any processing logic.

### Confluent Schema Registry
- Deploy Schema Registry as a Docker container alongside Kafka
- Define an **Avro schema** for your Kafka messages and register it
- Both the WebSocket producer and Spark consumer validate against the registry — schema mismatches fail loudly at the producer rather than silently corrupting downstream data
- For documentation, note that in a Python script (`confluent-kafka` library), this is a ~10 line addition; not a major implementation burden

### Kafka Message Schema (Avro)
```json
{
  "type": "record", "name": "OhlcvCandle",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "open", "type": "double"}, {"name": "high", "type": "double"},
    {"name": "low", "type": "double"}, {"name": "close", "type": "double"},
    {"name": "volume", "type": "double"},
    {"name": "open_time", "type": "long"}, {"name": "close_time", "type": "long"},
    {"name": "is_closed", "type": "boolean"}
  ]
}
```

### ClickHouse Tables

| Table | Layer | Engine | Description |
|---|---|---|---|
| `raw_ohlcv_1m` | Silver | **ReplacingMergeTree** | Cleaned 1-min candles — stream + batch write here |
| `agg_ohlcv_1h` | Gold | ReplacingMergeTree | Hourly aggregates from EMR batch |
| `agg_ohlcv_1d` | Gold | ReplacingMergeTree | Daily aggregates from EMR batch |
| `technical_features` | Gold | Materialized View | MAs, volatility, returns — auto-computed on insert |
| `pipeline_metrics` | Monitoring | MergeTree | Ingestion lag, row counts, bad record counts |
| `anomaly_events` | Gold | MergeTree | Flagged outliers |

**Why `ReplacingMergeTree`:** Both the WebSocket stream and the REST batch pipeline write to the same tables. The REST API provides the official final candle; `ReplacingMergeTree` ensures the same `(symbol, open_time)` written twice keeps only the latest version — no duplicates, no stream/batch conflicts.

All tables: partition by `toYYYYMM(open_time)`, order by `(symbol, open_time)`.

> **AWS Glue Data Catalog (optional):** Register your S3 Bronze and Silver Parquet schemas in Glue Data Catalog. Adds ~30 minutes of setup and lets you run ad-hoc Athena queries against S3 for debugging without spinning up Spark. Not required, but useful during development.

---

## Phase 3: Real-Time Ingestion

**Goal:** Stream live candle data from Binance into Kafka.

Tasks:
- Subscribe to `<symbol>@kline_1m` WebSocket streams for all 3 symbols
- Use an **async Kafka producer** (`aiokafka`) — a sync producer blocks the WebSocket event loop waiting for Kafka acknowledgement, causing lag and potential message loss
- Serialize messages using the Avro schema registered in Schema Registry
- Push each closed candle (`is_closed: true`) to Kafka topic `crypto_ohlcv_1m`
- Consumer group: allows Spark and any future consumers to read independently
- Kafka retention: 24–48 hours for replay and debugging

> **Kafka Connect note:** Document in your README that a production system would use Kafka Connect with a custom source connector rather than a Python script. Shows architectural awareness without adding scope.

---

## Phase 4: Stream Processing with Spark (Bronze → Silver)

**Goal:** Consume Kafka stream, clean and deduplicate, write to ClickHouse Silver.

**Scope discipline:** Clean and deduplicate only. No rolling analytics — those live in ClickHouse Materialized Views (Phase 5).

Tasks:
- Read from `crypto_ohlcv_1m` using Spark Structured Streaming
- Deserialize Avro using Schema Registry
- Validate schema; drop and count malformed rows (write bad record count to `pipeline_metrics`)
- Deduplicate by `(symbol, open_time)` with a 5-minute watermark for late messages
- Write cleaned rows to ClickHouse `raw_ohlcv_1m` (Silver layer)
- Also archive cleaned Parquet to S3 Silver: `s3://bucket/silver/symbol=.../year=.../month=.../`
- Configure **checkpointing to S3** (`s3://bucket/checkpoints/`) for fault-tolerant restarts

---

## Phase 5: Analytical Layer — ClickHouse Materialized Views (Gold)

**Goal:** Compute rolling analytics inside ClickHouse, not in Spark.

ClickHouse window functions execute incrementally on each insert into `raw_ohlcv_1m`, making the dashboard fast without any extra processing step.

Materialized Views to create:
- **Moving averages:** 5-period and 15-period MA on `close`
- **Rolling volatility:** rolling standard deviation of returns over a configurable window
- **Returns:** `(close - prev_close) / prev_close` per symbol

These feed directly into Grafana — no intermediate pipeline step required.

---

## Phase 6: Batch Processing with EMR (Bronze → Silver → Gold)

**Goal:** Scheduled Spark batch jobs over S3 historical data.

Tasks:
- Write Spark jobs for:
  - Bronze → Silver: validate, deduplicate, write cleaned Parquet to S3 Silver
  - Silver → Gold: compute hourly/daily aggregates → write to ClickHouse `agg_ohlcv_1h`, `agg_ohlcv_1d`
  - Data quality: gap detection, duplicate counts, range validation → write results to `pipeline_metrics`
  - Feature recomputation over historical windows when schema changes
- Submit as EMR Serverless runs (pay per vCPU-second, no idle cluster cost)

---

## Phase 7: Orchestration with Airflow

**Goal:** Schedule and monitor all batch workflows.

Run with `LocalExecutor` on EC2 Docker.

DAGs to build:
- **Daily aggregate refresh:** trigger EMR job → validate row counts via XCom → alert on failure
- **Weekly data quality check:** scan `raw_ohlcv_1m` for gaps, duplicates, out-of-range values
- **Backfill DAG:** parameterized by `start_date` / `end_date` for on-demand reprocessing

Airflow features to demonstrate:
- **S3KeySensor** — wait for Bronze Parquet files before triggering EMR
- **XComs** — pass row counts and quality check results between tasks
- **Retries with exponential backoff** on all EMR and API tasks

---

## Phase 8: Data Quality Layer

**Goal:** Make the pipeline trustworthy, not just functional.

Quality checks (run inside Airflow DAGs or as a dedicated Spark job, write results to `pipeline_metrics`):
- **Gap detection:** find missing 1-minute intervals per symbol
- **Duplicate detection:** verify `(symbol, open_time)` uniqueness post-dedup
- **Range validation:** flag candles where `low > high` or `volume = 0` unexpectedly
- **Freshness check:** alert if latest `open_time` in ClickHouse is more than 5 minutes behind wall clock (pipeline stalled)
- **Batch row count delta:** record Bronze row count vs Silver row count per job run — a large drop signals a validation issue

---

## Phase 9: Idempotency and Failure Recovery

**Goal:** Document and implement how the system recovers from each class of failure.

This doesn't require new infrastructure — it's about making existing components behave correctly under failure and documenting the recovery story explicitly.

| Failure scenario | Recovery mechanism |
|---|---|
| Stream job crashes | Spark restarts from S3 checkpoint — no data loss, no reprocessing |
| Stream job missed messages | Replay from Kafka retention window (24–48hr) |
| Batch job writes duplicate rows | `ReplacingMergeTree` keeps latest version by `(symbol, open_time)` — idempotent by design |
| Corrupted date range in ClickHouse | Airflow backfill DAG reruns only that date window; `ReplacingMergeTree` overwrites stale rows |
| EMR job fails mid-run | Airflow retries with exponential backoff; S3 Bronze is unchanged so rerun is safe |
| Historical pull interrupted | Script is resumable — track last pulled timestamp, restart from there |

Document this table in your README. It is one of the most common interview/presentation questions for data engineering projects.

---

## Phase 10: Monitoring and Observability

**Goal:** Show the pipeline is a live, healthy system — not just a one-time demo.

This phase is intentionally last because it depends on all prior phases being built. It is a cross-cutting concern, not a standalone system.

### What to track (write to `pipeline_metrics` ClickHouse table)
- Ingestion lag: wall clock time minus latest `open_time` per symbol
- Latest candle timestamp per symbol
- Bad record count per Spark micro-batch
- Duplicate count per batch job run
- Missing minute count per symbol per day
- Batch row count before vs after Spark validation
- Stream processing latency (message timestamp → ClickHouse write timestamp)

### Implementation
- **Spark streaming job:** write metric rows to `pipeline_metrics` on each micro-batch commit
- **Airflow DAGs:** write quality check results to `pipeline_metrics` via a final task in each DAG
- **Prometheus (optional):** expose a `/metrics` endpoint from a lightweight Python sidecar if you want alerting; Grafana can scrape it directly. Not required for the project — the ClickHouse-based dashboard is sufficient.

### Grafana — Pipeline Health Dashboard (separate from analytics dashboard)
Build this as a **second Grafana dashboard**, distinct from the price/analytics one:
- Ingestion lag per symbol (should stay near 0 for live stream)
- Latest candle timestamp per symbol (confirms pipeline is live)
- Bad record rate over time
- Missing minute count per day
- Batch job row count before vs after (delta signals data quality issues)

This dashboard is what makes the project look like a real monitored data platform rather than a demo that happened to produce some charts.

---

## AWS Cost Guide ($300 credits)

| Service | Usage | Estimated Cost |
|---|---|---|
| EC2 t3.xlarge × 1 | ~10 hrs/week × 12 weeks | ~$20 |
| EC2 t3.xlarge × 2 (if needed) | ~10 hrs/week × 12 weeks | ~$40 |
| S3 | ~15GB Bronze + Silver | ~$2–4/month |
| EMR Serverless | ~5 jobs/week, ~10 min each | ~$5–15/month |
| Data transfer | Minimal at this scale | ~$2–5 |
| **Total estimate (1 machine)** | | **~$50–80 for the semester** |
| **Total estimate (2 machines)** | | **~$90–130 for the semester** |

**Do not use:**
- MSK (Managed Kafka) — ~$0.21/hr per broker minimum, ~$180+ for the semester
- MWAA (Managed Airflow) — ~$0.49/hr environment baseline, ~$420+ for the semester

---

## Final Stack

| Layer | Tool | Notes |
|---|---|---|
| Data source | Binance REST + WebSocket | One source, both patterns |
| Schema contract | Confluent Schema Registry | Avro, validates producer + consumer |
| Message bus | Kafka (Docker on EC2) | Async producer |
| Stream processing | Spark Structured Streaming | Clean + dedup only |
| Analytics | ClickHouse Materialized Views | Rolling windows, MAs (Gold layer) |
| Batch processing | Spark on EMR Serverless | Bronze → Silver → Gold |
| Raw storage | S3 Bronze (Parquet) | Immutable source of truth |
| Cleaned storage | S3 Silver (Parquet) | Post-validation, deduplicated |
| Analytical storage | ClickHouse ReplacingMergeTree | Handles stream + batch upserts |
| Orchestration | Airflow LocalExecutor (Docker) | Low RAM, sensors + XComs |
| Dashboarding | Grafana + ClickHouse plugin | Analytics dashboard + health dashboard |
| Metadata (optional) | AWS Glue Data Catalog | S3 schema discovery, enables Athena |

---

## Optional Extensions (after core pipeline works)
- Anomaly detection on price/volume spikes → `anomaly_events` table
- Prometheus sidecar + alerting rules for ingestion lag thresholds
- Simple ML feature store for forecasting experiments
- Add more symbols or a second exchange for cross-venue comparison
