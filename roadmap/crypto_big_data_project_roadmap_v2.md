# Crypto Big Data Project Roadmap v2

## Architecture Overview

### Streaming Pipeline
```
Binance WebSocket
→ Kafka (Docker, local EC2)
→ Spark Structured Streaming
→ ClickHouse
→ Grafana
```

### Batch Pipeline
```
Binance REST API (with rate-limit handling)
→ S3 (raw Parquet)
→ Airflow (orchestration)
→ EMR (Spark batch jobs)
→ ClickHouse aggregate tables
→ Grafana
```

### Scope
- **Symbols:** BTCUSDT, ETHUSDT, SOLUSDT (3 pairs to start)
- **Granularity:** 1-minute OHLCV
- **Derived features:** returns, rolling averages, rolling volatility
- **Infra:** Single EC2 instance (t3.xlarge) for streaming stack; S3 + EMR for batch

---

## Phase 0: Infrastructure Setup

**Goal:** Get local/cloud environment ready before writing any pipeline code.

Tasks:
- Provision EC2 instance (t3.xlarge) on AWS
- Set up Docker Compose on EC2 with: Kafka, Spark, ClickHouse, Airflow, Grafana
- Create S3 bucket for raw data lake (`s3://your-project/raw/`)
- Set up IAM roles for EC2 → S3 and EMR → S3 access
- Define and document your schemas upfront:
  - Kafka message schema (JSON fields: symbol, open, high, low, close, volume, timestamp)
  - ClickHouse table schemas (see Phase 5)
  - Parquet file layout: `s3://bucket/raw/symbol=BTCUSDT/year=2024/month=01/`

---

## Phase 1: Historical Data Ingestion

**Goal:** Build a backfill dataset in S3 using the Binance REST API.

Tasks:
- Write a Python script to pull 1-minute candles via `GET /api/v3/klines`
- Implement **rate limit handling**: Binance allows 1200 request weight/min; each klines call costs 2 weight. Add `time.sleep()` between batches and read the `X-MBN-USED-WEIGHT` response header to back off when close to the limit
- Pull ~3 months of history per symbol
- Save output as partitioned Parquet to S3: `s3://bucket/raw/symbol=BTCUSDT/year=.../month=.../`
- Run a quick sanity check: row counts, date coverage, null values

> **Note on Kaggle:** You can optionally use a Kaggle dataset to bootstrap your ClickHouse and Spark testing in Phases 2–4 *before* your API pull finishes. Just replace it with your own API data before final submission — showing real ingestion code matters for the grade.

---

## Phase 2: Schema and Data Contract Design

**Goal:** Lock in your data contracts before building processing logic.

Define:
- Kafka message format (JSON): `{ symbol, open, high, low, close, volume, open_time, close_time, is_closed }`
- ClickHouse tables:

| Table | Description |
|---|---|
| `raw_ohlcv_1m` | Raw ingested 1-minute candles |
| `agg_ohlcv_1h` | Hourly aggregates |
| `agg_ohlcv_1d` | Daily aggregates |
| `technical_features` | Rolling metrics (MA, volatility, returns) |
| `anomaly_events` | Flagged outliers |

- All ClickHouse tables should use **MergeTree** engine, partitioned by `toYYYYMM(open_time)` and ordered by `(symbol, open_time)`
- Document schemas in a `schemas/` folder in your repo

---

## Phase 3: Real-Time Ingestion

**Goal:** Stream live candle data from Binance into Kafka.

Tasks:
- Subscribe to `<symbol>@kline_1m` WebSocket streams
- Push each closed candle as a JSON message to Kafka topic `crypto_ohlcv_1m`
- Use a consumer group so Spark and any other consumers can read independently
- Keep Kafka retention at 24–48 hours (enough for replay/debugging)

---

## Phase 4: Stream Processing with Spark

**Goal:** Consume Kafka stream, clean it, and write to ClickHouse in near-real-time.

Tasks:
- Read from `crypto_ohlcv_1m` Kafka topic using Spark Structured Streaming
- Parse and validate JSON schema; drop malformed rows
- Deduplicate by `(symbol, open_time)`
- Compute rolling features: 5-min/15-min moving averages, returns, rolling std dev
- Write processed rows to ClickHouse `raw_ohlcv_1m` and `technical_features`
- Configure **checkpointing** to S3 (`s3://bucket/checkpoints/`) for fault tolerance — this ensures the stream can resume after a restart without reprocessing or losing data

---

## Phase 5: Batch Processing with EMR

**Goal:** Use EMR for scheduled Spark batch jobs over historical data.

Tasks:
- Write Spark batch jobs for:
  - Hourly and daily aggregate generation from `raw_ohlcv_1m`
  - Data quality checks (gap detection, duplicate counts, out-of-range values)
  - Feature recomputation over historical windows
- Submit jobs as EMR Steps (or use EMR Serverless for pay-per-use pricing)
- Read from S3 Parquet; write aggregates back to ClickHouse
- Store intermediate outputs back to S3 if needed

> **EMR Serverless** is worth considering for batch jobs — you pay per vCPU-second only when jobs run, which is cheaper than keeping a cluster alive.

---

## Phase 6: Orchestration with Airflow

**Goal:** Schedule and monitor all batch workflows.

DAGs to build:
- **Daily aggregate refresh:** trigger EMR batch job → validate row counts → alert on failure
- **Weekly data quality check:** scan for gaps, duplicates, and anomalies in `raw_ohlcv_1m`
- **Backfill DAG:** parameterized DAG to pull and reprocess a date range on demand

Airflow features to use:
- **Sensors** to wait for S3 files before triggering Spark jobs
- **XComs** to pass row counts or validation results between tasks
- **Retries with exponential backoff** on all EMR and external API tasks

---

## Phase 7: Data Quality Layer

**Goal:** Make the pipeline trustworthy, not just functional.

Checks to implement (can live in Airflow DAGs or as a standalone Spark job):
- **Gap detection:** find missing 1-minute intervals per symbol
- **Duplicate detection:** check for repeated `(symbol, open_time)` pairs
- **Range validation:** flag candles where low > high, or volume = 0 unexpectedly
- **Late data handling:** in the Spark stream, set a watermark (e.g. 5 minutes) to handle slightly delayed messages

---

## Phase 8: Dashboard with Grafana

**Goal:** Visualise pipeline outputs for the demo.

Setup:
- Install the **ClickHouse data source plugin** for Grafana (official plugin, 2 minutes to configure)
- Connect directly to ClickHouse — no intermediate layer needed

Panels to build:
- Live 1-minute price chart per symbol
- Rolling 15-minute volatility
- Hourly volume bars
- Moving average overlay (5-min, 15-min)
- Data quality panel: gap count and latest ingestion timestamp (shows the pipeline is healthy)

> Grafana is the right choice here. It has a native ClickHouse plugin, time-series panels work out of the box, and there's no application server or auth setup required — unlike Superset or Plotly/Dash.

---

## AWS Cost Guide (for $300 credits)

| Service | Usage | Estimated Cost |
|---|---|---|
| EC2 t3.xlarge | ~10 hrs/week for 12 weeks | ~$20 |
| S3 | ~10GB raw + aggregates | ~$1–2/month |
| EMR Serverless | ~5 batch jobs/week, ~10 min each | ~$5–15/month |
| Data transfer | Minimal at this scale | ~$2–5 |
| **Total estimate** | | **~$50–80 for the semester** |

**Do not use:**
- MSK (Managed Kafka) — ~$0.21/hr per broker, expensive for a dev project
- MWAA (Managed Airflow) — ~$0.49/hr just for the environment

Run Kafka, Airflow, and ClickHouse on Docker on your EC2 instance instead.

---

## Final Stack

| Layer | Tool |
|---|---|
| Data source | Binance REST + WebSocket |
| Message bus | Kafka (Docker on EC2) |
| Stream processing | Spark Structured Streaming |
| Batch processing | Spark on EMR (Serverless) |
| Raw storage | S3 (Parquet, partitioned) |
| Analytical storage | ClickHouse (MergeTree) |
| Orchestration | Airflow (Docker on EC2) |
| Dashboarding | Grafana + ClickHouse plugin |

---

## Optional Extensions (after core pipeline works)
- Anomaly detection on price/volume spikes → write to `anomaly_events` table
- Simple ML feature store for forecasting experiments
- Pipeline health monitoring panel in Grafana (lag, job durations, error rates)
- Add more symbols or a second exchange for comparison
