# Crypto Big Data Project Roadmap

## Project Goal
Build a **big data pipeline for 1-minute crypto market data** that demonstrates both:
- **stream processing** for near-real-time updates
- **batch processing** for historical reprocessing, quality checks, and aggregate generation

The goal is not just to make charts, but to show a **complete data engineering architecture** from ingestion to storage to analytics to dashboarding.

---

## Recommended Architecture

### Streaming pipeline
```text
Binance WebSocket
→ Kafka
→ Spark Structured Streaming
→ ClickHouse
→ Grafana / Superset
```

### Batch pipeline
```text
Binance REST / archived raw data
→ S3 or local Parquet
→ Airflow
→ Spark batch jobs
→ ClickHouse aggregate tables
→ Dashboard
```

### Why split it this way?
This gives the project a stronger big-data story:
- the **streaming side** shows event-driven ingestion and near-real-time processing
- the **batch side** shows orchestration, backfilling, data quality checks, and recomputation

This makes the architecture look much more complete than a single notebook or a CSV-based workflow.

---

## End-to-End Roadmap

### Phase 1: Define scope
Keep the project realistic and manageable.

Recommended scope:
- 2 to 5 trading pairs, such as BTCUSDT, ETHUSDT, SOLUSDT
- 1-minute OHLCV data only
- a few derived features such as returns, rolling averages, or volatility
- a simple but working dashboard

This is enough to look serious without making the system too heavy for a single laptop.

---

### Phase 2: Historical data ingestion
Use historical data first so the project has enough data to process before live streaming is added.

Plan:
1. Pull historical 1-minute candles from Binance REST
2. Save the raw data to Parquet files locally or to S3
3. Use this dataset for early Spark and ClickHouse testing

Why this matters:
- gives you a backfill dataset
- lets you debug pipeline logic before handling live traffic
- supports batch jobs later

---

### Phase 3: Real-time ingestion
Once historical ingestion works, add the live pipeline.

Plan:
1. Subscribe to Binance WebSocket streams
2. Push incoming messages into Kafka
3. Keep the Kafka topic as the buffer between ingestion and processing

Suggested topic:
- `crypto_ohlcv_1m`

Why Kafka is useful here:
- decouples ingestion from downstream processing
- makes the system look like a real streaming platform
- allows multiple consumers later if needed

---

### Phase 4: Stream processing with Spark
Use Spark Structured Streaming to consume the Kafka stream.

Tasks Spark can do:
- parse JSON messages
- validate schema
- remove bad or duplicate rows
- compute rolling metrics
- aggregate to hourly summaries
- write processed output to ClickHouse

This is where the project becomes more than just data collection.

---

### Phase 5: Analytical storage with ClickHouse
Use ClickHouse as the serving layer for dashboards and fast time-series analytics.

Suggested tables:
- `raw_ohlcv_1m`
- `agg_ohlcv_1h`
- `agg_ohlcv_1d`
- `technical_features`
- `anomaly_events`

Why ClickHouse fits well:
- fast for aggregations over time-series data
- well suited for dashboards
- easier to justify than Cassandra for analytical queries

---

### Phase 6: Batch orchestration with Airflow
Use Airflow for the scheduled side of the platform.

Good Airflow jobs:
- historical backfills
- hourly or daily aggregate refresh jobs
- data quality checks
- missing-partition recovery
- feature generation

Important note:
Airflow is **not** the streaming engine. It does not replace Kafka or Spark Streaming. It should sit on the **batch/orchestration side**, not in the middle of the live data flow.

---

### Phase 7: Dashboard and presentation layer
Use a dashboard tool to present the final outputs.

Good options:
- **Grafana** for live charts and time-series panels
- **Apache Superset** for BI-style dashboards

Possible dashboard metrics:
- live 1-minute prices
- rolling volatility
- hourly volume
- moving averages
- anomaly alerts
- top symbols by activity

---

## Why Binance Was Recommended

Binance was chosen mainly because it is practical for a student big-data project.

### Advantages of Binance
- popular source for crypto market data
- provides both **REST** and **WebSocket** interfaces
- widely used in tutorials and open-source examples
- good coverage for 1-minute market data
- easier to prototype quickly than many alternatives

### Why Binance is a good project fit
You need both:
- **historical data** for backfilling and batch processing
- **live updates** for the streaming pipeline

Binance supports both patterns cleanly, which makes the architecture easier to build and explain.

### Why not rely on many APIs at once?
Using too many data providers adds complexity without adding much academic value.

Problems with using many APIs:
- more authentication and setup work
- different schemas and rate limits
- harder debugging
- more time spent on integration rather than pipeline design

For a school project, one good source is usually enough.

---

## Why Kafka Was Chosen
Kafka is useful because it separates producers from consumers.

Instead of this:
```text
Binance → Spark directly
```

You get this:
```text
Binance → Kafka → Spark / archive / other consumers
```

That gives you:
- buffering
- decoupling
- replayability
- a more realistic data platform architecture

For a big-data project, Kafka adds real architectural value.

---

## Why Spark Was Chosen
Spark is a strong choice because it can cover both:
- **streaming** with Structured Streaming
- **batch** with normal Spark jobs

This is useful for your project because you want both kinds of pipelines.

Other benefits:
- widely recognized in big data
- easy to justify academically
- works with Kafka, Parquet, S3, and ClickHouse
- scalable conceptually even if your demo is small

---

## Why ClickHouse Was Chosen
ClickHouse is strong for analytical workloads, especially time-series style queries.

Typical needs in your project:
- aggregate over time windows
- filter by symbol and timestamp
- power charts and dashboard queries

ClickHouse handles these very well.

Compared with forcing a more general-purpose distributed database into the project, ClickHouse gives you a cleaner story for analytics and dashboarding.

---

## Why Airflow Was Chosen
Airflow helps when you have **multi-step scheduled workflows**.

Examples:
- fetch historical data
- validate the files
- trigger Spark batch job
- load aggregates into ClickHouse
- run data quality checks

This is much more expressive than simple cron jobs because Airflow provides:
- dependency management
- retries
- logging
- scheduling
- visual workflow monitoring

So Airflow is useful, but only where orchestration is needed.

---

## Why Cassandra Might Not Be Suitable
Cassandra is powerful, but it is often recommended for the wrong reasons.

### Where Cassandra is strong
Cassandra is good when you need:
- very high write throughput
- distributed writes across nodes/regions
- highly available operational workloads
- predictable key-based access patterns

### Why it may not fit this project well
Your project sounds more like an **analytics and pipeline** problem than a distributed operational database problem.

Likely needs:
- aggregations over time
- dashboard queries
- historical analysis
- batch recomputation

Cassandra is usually **not the most natural fit** for those needs because:
- ad hoc analytical querying is weaker
- dashboards and aggregates are not its main strength
- it adds modeling complexity
- it is harder to justify unless the project is explicitly about high-scale operational serving

So Cassandra is not “bad”; it just does not match the main goal as well as ClickHouse does.

---

## Why Flink Might Not Be Suitable
Flink is a very strong stream-processing tool, but it may be too much for this project.

### Why people consider Flink
Flink is often preferred for:
- advanced event-time processing
- very low-latency streaming
- complex stateful streaming jobs
- sophisticated streaming-first architectures

### Why it may not be the best choice here
For your project:
- Spark can already handle both batch and streaming
- adding Flink increases tool complexity
- the educational benefit may not justify the extra setup burden
- the project may become harder to debug and explain

So Flink is a great tool, but unless your project specifically emphasizes advanced streaming semantics, Spark is the simpler and more balanced choice.

---

## Why EMR Might Not Be Suitable
Amazon EMR is a managed cluster service for running tools like Spark and Hadoop on AWS.

### Why EMR sounds attractive
- cloud-based cluster management
- supports Spark and related tools
- useful at larger scale

### Why it may not be necessary here
For a student project:
- you can run a smaller version locally on one laptop
- Docker-based local setup is easier to demo and control
- EMR introduces cloud setup and cost-management overhead
- cluster management can distract from the pipeline itself

### Better use of AWS credits
A more practical use of AWS credits would be:
- **S3 for raw data storage**
- optional EC2 only if your laptop cannot handle the workload

That gives you a hybrid setup without overcomplicating the project.

---

## Can This Be Done on One Laptop?
Yes, in a reduced-scale version.

You can usually run these locally with Docker Compose:
- Kafka
- Spark
- ClickHouse
- Airflow
- Grafana

What you should reduce:
- number of symbols
- retention length
- frequency of heavy jobs
- Spark resource allocation

That means you are building a **small but valid version of an industry architecture**, which is perfectly acceptable for a school project.

---

## Suggested Final Stack

### Core stack
- **Binance** for market data
- **Kafka** for event streaming
- **Spark Structured Streaming + Spark batch** for processing
- **S3 or Parquet** for raw storage
- **ClickHouse** for analytics storage
- **Airflow** for orchestration
- **Grafana or Superset** for dashboards

### Why this stack is strong
It demonstrates the full lifecycle:
- ingestion
- transport
- stream processing
- batch processing
- storage
- orchestration
- visualization

That makes the project look like a real data platform rather than a notebook-based analysis.

---

## Final Recommendation
The most balanced architecture is:

```text
[Streaming]
Binance WebSocket
→ Kafka
→ Spark Structured Streaming
→ ClickHouse
→ Grafana

[Batch]
Binance REST / archived raw data
→ S3 or Parquet
→ Airflow
→ Spark batch
→ ClickHouse aggregate tables
→ Dashboard
```

This design is recommended because it is:
- realistic
- technically defensible
- strong for a big-data project
- possible to prototype on a single laptop
- easy to scale conceptually if you want to discuss future improvements

---

## Optional Future Extensions
If you want to extend the project later, possible additions include:
- anomaly detection
- simple ML features or forecasting
- more symbols or exchanges
- data quality alerting
- partitioned lake storage layout
- monitoring for pipeline health

These are good extensions, but they should come **after** the core pipeline works.

---

## One-Sentence Summary
Build a **hybrid batch + streaming crypto data platform** using **Binance, Kafka, Spark, ClickHouse, Airflow, and a dashboard**, while avoiding unnecessary complexity from tools like **Cassandra, Flink, or EMR** unless the project scope explicitly requires them.
