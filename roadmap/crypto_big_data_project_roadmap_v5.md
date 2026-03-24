# Crypto Big Data Project Roadmap v5

## Architecture Overview

### Streaming Pipeline
```
Binance WebSocket
→ Kinesis Data Stream (async Python producer)
→ Kinesis Firehose (S3 Bronze archival — automatic)
→ Spark Structured Streaming (reads from Kinesis, clean + deduplicate only)
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
├── bronze/   ← raw, unmodified: REST Parquet + Firehose stream archive
├── silver/   ← cleaned, deduplicated Parquet after Spark validation
└── gold/     ← not stored in S3; served from ClickHouse aggregates
```

### Scope
- **Symbols:** BTCUSDT, ETHUSDT, SOLUSDT (3 pairs)
- **Granularity:** 1-minute OHLCV
- **Derived features:** returns, rolling MAs, rolling volatility (ClickHouse Materialized Views)
- **Infra:** Single EC2 t3.xlarge for streaming stack; Kinesis + S3 + EMR Serverless on AWS

---

## Phase 0: Infrastructure Setup

**Goal:** Get environment ready before writing any pipeline code.

Tasks:
- Provision EC2 t3.xlarge on AWS (16GB RAM — see memory limits below)
- Set up Docker Compose on EC2 with: Spark, ClickHouse, Airflow, Grafana
- **Docker memory limits:** Set hard limits in `docker-compose.yml` to prevent OOM kills:

| Service | Memory Limit |
|---|---|
| Spark (driver + executor) | 4GB total |
| ClickHouse | 4GB |
| Airflow (LocalExecutor) | 2GB |
| Grafana | 512MB |

> **Note:** Kafka and Zookeeper are no longer in the Docker stack — Kinesis is fully managed on AWS, so RAM pressure on EC2 is meaningfully reduced compared to running Kafka locally.

- **Spark config:** `spark.driver.memory=1g`, `spark.executor.memory=2g`
- **Swap space:** Enable a 4–8GB swap file on the EC2 instance as a safety net
- **Airflow:** Use `LocalExecutor` (not CeleryExecutor) — lower RAM footprint

AWS setup:
- Create Kinesis Data Stream: `crypto-ohlcv-1m`, **1 shard** (sufficient for 3 symbols at 1-min granularity)
- Create Kinesis Firehose delivery stream: source = `crypto-ohlcv-1m`, destination = S3 Bronze (`s3://your-project/bronze/stream/`)
  - Set Firehose buffer: 60 seconds or 5MB — whichever comes first
  - Enable **Glue Schema Registry** on the Firehose stream for schema validation (see Phase 2)
- Create S3 bucket with prefixes: `s3://your-project/bronze/`, `/silver/`
- Set up IAM roles: EC2 → Kinesis + S3, EMR → S3
- Document schemas in a `schemas/` folder in your repo

> **Local machine note:** Same RAM constraints apply during local development. With Kinesis managed on AWS, local Docker is lighter. Use EC2 for the final demo run.

> **Two-machine option:** If you hit RAM pressure, split into Machine 1 (Airflow + Grafana) and Machine 2 (Spark + ClickHouse). Both t3.xlarge at 10hrs/week runs ~$40 total for the semester.

---

## Phase 1: Historical Data Ingestion (Bronze)

**Goal:** Pull raw historical data from Binance and land it in S3 Bronze.

Tasks:
- Write a Python script to pull 1-minute candles via `GET /api/v3/klines`
- **Rate limit handling:** Binance allows 1200 request weight/min; each klines call costs 2 weight. Read the `X-MBN-USED-WEIGHT` response header and back off dynamically — don't use a fixed `time.sleep()`
- Pull ~3 months of history per symbol
- Save raw output as partitioned Parquet to S3 Bronze: `s3://bucket/bronze/rest/symbol=BTCUSDT/year=.../month=.../`
- Sanity check: row counts, date coverage, null values

> **Note on Kaggle:** You can use a Kaggle dataset to bootstrap ClickHouse and Spark testing in early phases. Replace with real API data before final submission.

---

## Phase 2: Schema and Data Contract Design

**Goal:** Lock in data contracts before building any processing logic.

### AWS Glue Schema Registry
- Replaces Confluent Schema Registry (which is Kafka-specific)
- Register your message schema in Glue Schema Registry — Kinesis Firehose integrates with it natively for validation
- Both the WebSocket producer (via `boto3` + `aws-glue-schema-registry` library) and Spark consumer validate against the registry
- Schema mismatches are caught at the producer before bad data enters the stream

### Kinesis Message Schema (JSON)
```json
{
  "symbol": "BTCUSDT",
  "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05,
  "volume": 100.0,
  "open_time": 1700000000000,
  "close_time": 1700000059999,
  "is_closed": true
}
```

> Kinesis works with JSON natively — Avro is possible but adds complexity without meaningful benefit at this scale. JSON with Glue Schema Registry validation is the right choice here.

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

> **AWS Glue Data Catalog (optional):** Register your S3 Bronze and Silver Parquet schemas here too. Lets you run ad-hoc Athena queries against S3 for debugging without spinning up Spark.

---

## Phase 3: Real-Time Ingestion

**Goal:** Stream live candle data from Binance into Kinesis Data Streams.

Tasks:
- Subscribe to `<symbol>@kline_1m` WebSocket streams for all 3 symbols
- Use an **async Kinesis producer** with `aiobotocore` or run `boto3` in a thread pool — same principle as an async Kafka producer: don't block the WebSocket event loop waiting for Kinesis to acknowledge a PUT
- On each closed candle (`is_closed: true`), call `kinesis.put_record()` with:
  - `StreamName`: `crypto-ohlcv-1m`
  - `Data`: JSON-serialised candle
  - `PartitionKey`: symbol name (e.g. `"BTCUSDT"`) — this routes same-symbol records to the same shard, preserving order
- Kinesis Firehose automatically archives all stream records to S3 Bronze — no separate archival code needed

**Kinesis vs Kafka operational notes:**
- Kinesis uses **shards** instead of consumer groups — 1 shard handles up to 1MB/s ingest and 2MB/s read, more than sufficient for this project
- Retention: **24 hours by default**, extendable to 7 days for an extra cost. Plan your replay window accordingly (see Phase 9)
- No broker to manage, no Zookeeper — the stream is fully managed

---

## Phase 4: Stream Processing with Spark (Stream → Silver)

**Goal:** Consume Kinesis stream, clean and deduplicate, write to ClickHouse Silver.

**Scope discipline:** Clean and deduplicate only. No rolling analytics — those live in ClickHouse Materialized Views (Phase 5).

Tasks:
- Read from Kinesis Data Stream using Spark Structured Streaming with the **`spark-sql-kinesis`** connector:
  ```
  spark.readStream \
    .format("kinesis") \
    .option("streamName", "crypto-ohlcv-1m") \
    .option("region", "ap-southeast-1") \
    .option("initialPosition", "latest") \
    .load()
  ```
- Parse JSON, validate schema; drop and count malformed rows (write bad record count to `pipeline_metrics`)
- Deduplicate by `(symbol, open_time)` with a 5-minute watermark for late messages
- Write cleaned rows to ClickHouse `raw_ohlcv_1m` (Silver layer)
- Also write cleaned Parquet to S3 Silver: `s3://bucket/silver/symbol=.../year=.../month=.../`
- Configure **checkpointing to S3** (`s3://bucket/checkpoints/`) for fault-tolerant restarts

> **Note on `spark-sql-kinesis`:** This connector is less mature than Spark's native Kafka connector. Test it early in development — don't leave Kinesis integration to the last week.

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

Run with `LocalExecutor` on EC2 Docker. Airflow submits jobs to EMR Serverless and monitors their completion — it does not do any data processing itself.

DAGs to build:
- **Daily aggregate refresh:** trigger EMR job → validate row counts via XCom → alert on failure
- **Weekly data quality check:** scan `raw_ohlcv_1m` for gaps, duplicates, out-of-range values
- **Backfill DAG:** parameterized by `start_date` / `end_date` for on-demand reprocessing

Airflow features to demonstrate:
- **S3KeySensor** — wait for Bronze Parquet files before triggering EMR
- **EmrServerlessStartJobRunOperator** — submit Spark jobs to EMR; Airflow waits for completion
- **XComs** — pass row counts and quality check results between tasks
- **Retries with exponential backoff** on all EMR and API tasks

---

## Phase 8: Data Quality Layer

**Goal:** Make the pipeline trustworthy, not just functional.

Quality checks (run inside Airflow DAGs or as a dedicated Spark job, write results to `pipeline_metrics`):
- **Gap detection:** find missing 1-minute intervals per symbol
- **Duplicate detection:** verify `(symbol, open_time)` uniqueness post-dedup
- **Range validation:** flag candles where `low > high` or `volume = 0` unexpectedly
- **Freshness check:** alert if latest `open_time` in ClickHouse is more than 5 minutes behind wall clock
- **Batch row count delta:** record Bronze vs Silver row count per job run — a large drop signals a validation issue

---

## Phase 9: Idempotency and Failure Recovery

**Goal:** Document and implement how the system recovers from each class of failure.

| Failure scenario | Recovery mechanism |
|---|---|
| Stream job crashes | Spark restarts from S3 checkpoint — no data loss, no reprocessing |
| Stream job missed messages | Replay from Kinesis retention window (**24hrs default, up to 7 days**) — plan demo windows accordingly |
| Kinesis shard unavailable | AWS manages shard availability; producer retries via `boto3` retry config |
| Batch job writes duplicate rows | `ReplacingMergeTree` keeps latest version by `(symbol, open_time)` — idempotent by design |
| Corrupted date range in ClickHouse | Airflow backfill DAG reruns only that date window; `ReplacingMergeTree` overwrites stale rows |
| EMR job fails mid-run | Airflow retries with exponential backoff; S3 Bronze is unchanged so rerun is safe |
| Historical pull interrupted | Script is resumable — track last pulled timestamp, restart from there |
| Firehose delivery failure | Firehose retries automatically and writes failed records to a separate S3 error prefix |

> **Key difference from Kafka:** Kinesis retention maxes at 7 days (vs Kafka's configurable unlimited). For stream replay beyond 7 days, fall back to the S3 Bronze archive that Firehose writes automatically.

Document this table in your README — it is one of the most common presentation and interview questions for data engineering projects.

---

## Phase 10: Monitoring and Observability

**Goal:** Show the pipeline is a live, healthy system — not just a one-time demo.

This phase is intentionally last because it depends on all prior phases. It is a cross-cutting concern, not a standalone system.

### What to track (write to `pipeline_metrics` ClickHouse table)
- Ingestion lag: wall clock time minus latest `open_time` per symbol
- Latest candle timestamp per symbol
- Bad record count per Spark micro-batch
- Duplicate count per batch job run
- Missing minute count per symbol per day
- Batch row count before vs after Spark validation
- Stream processing latency (Kinesis PUT timestamp → ClickHouse write timestamp)

### Implementation
- **Spark streaming job:** write metric rows to `pipeline_metrics` on each micro-batch commit
- **Airflow DAGs:** write quality check results to `pipeline_metrics` via a final task in each DAG
- **Prometheus (optional):** lightweight Python sidecar exposing `/metrics` for Grafana to scrape — not required, ClickHouse-based dashboard is sufficient

### Grafana — Pipeline Health Dashboard (separate from analytics dashboard)
- Ingestion lag per symbol (should stay near 0 for live stream)
- Latest candle timestamp per symbol (confirms pipeline is live)
- Bad record rate over time
- Missing minute count per day
- Batch job row count delta (Bronze vs Silver)

---

## AWS Cost Guide ($300 credits)

| Service | Usage | Estimated Cost |
|---|---|---|
| EC2 t3.xlarge × 1 | ~10 hrs/week × 12 weeks | ~$20 |
| EC2 t3.xlarge × 2 (if needed) | ~10 hrs/week × 12 weeks | ~$40 |
| Kinesis Data Stream | 1 shard × ~10 hrs/week × 12 weeks | ~$5–8 |
| Kinesis Firehose | ~1GB/month ingested | ~$2–3/month |
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
| Schema contract | AWS Glue Schema Registry | JSON validation, native Kinesis integration |
| Message bus | Kinesis Data Streams | 1 shard, async boto3 producer |
| Stream archival | Kinesis Firehose | Auto-delivers stream to S3 Bronze |
| Stream processing | Spark Structured Streaming | Reads from Kinesis, clean + dedup only |
| Analytics | ClickHouse Materialized Views | Rolling windows, MAs (Gold layer) |
| Batch processing | Spark on EMR Serverless | Bronze → Silver → Gold |
| Raw storage | S3 Bronze (Parquet + Firehose archive) | Immutable source of truth |
| Cleaned storage | S3 Silver (Parquet) | Post-validation, deduplicated |
| Analytical storage | ClickHouse ReplacingMergeTree | Handles stream + batch upserts |
| Orchestration | Airflow LocalExecutor (Docker) | Submits to EMR, sensors + XComs |
| Dashboarding | Grafana + ClickHouse plugin | Analytics dashboard + health dashboard |
| Metadata (optional) | AWS Glue Data Catalog | S3 schema discovery, enables Athena |

---

## Optional Extensions (after core pipeline works)
- Anomaly detection on price/volume spikes → `anomaly_events` table
- Prometheus sidecar + alerting rules for ingestion lag thresholds
- Simple ML feature store for forecasting experiments
- Add more symbols or a second exchange for cross-venue comparison
