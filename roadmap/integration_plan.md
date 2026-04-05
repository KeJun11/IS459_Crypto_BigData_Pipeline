# Phased Integration Plan For Shared ClickHouse And Streaming Dashboard

## Summary
Implement the integration in four execution phases, then leave Grafana Cloud and S3-bucket alignment as explicit follow-up decisions. The first objective is to move the existing working streaming pipeline onto Henry’s shared ClickHouse server using a separate database, without changing ingestion semantics. Only after the shared data path is stable should the work expand into a finance-style dashboard and a larger symbol set.

This plan assumes:
- shared ClickHouse is the first integration target
- your pipeline will use its own dedicated ClickHouse database on that server
- self-hosted Grafana remains the active dashboard frontend for now
- Grafana Cloud is deferred until after the shared ClickHouse + dashboard path is stable

## Phase Plan

### Phase 1: Shared ClickHouse Cutover
Goal: move your pipeline’s serving/storage target from your current ClickHouse instance to Henry’s shared ClickHouse server, while keeping your schemas isolated.

Implementation:
- Create a dedicated database on the shared ClickHouse server for your pipeline.
- Apply your existing ClickHouse DDLs into that new database:
  - `raw_ohlcv_1m`
  - `pipeline_metrics`
  - `agg_ohlcv_1h`
  - `agg_ohlcv_1d`
  - `technical_features`
- Do not rename your application tables unless the shared server imposes a naming constraint.
- Repoint all runtime config that currently assumes your own `crypto` database:
  - stream consumer ClickHouse host/port/database
  - Grafana datasource database
  - any batch jobs that still write to ClickHouse
- Keep your current schema definitions unchanged during cutover unless there is a hard compatibility issue.
- Confirm the shared ClickHouse instance can read or write correctly with its current credentials and network policy.

Interfaces / config decisions:
- Keep your own database name on the shared server rather than adapting to Henry’s existing `cleaned_crypto` and `ohlcv_1min` layout.
- Continue writing through your application code; do not introduce manual load steps as part of normal runtime.
- Only use manual ClickHouse queries for one-time setup/verification.

Acceptance criteria:
- your streaming consumer writes successfully into the new shared ClickHouse database
- `raw_ohlcv_1m` and `pipeline_metrics` populate there
- no schema collisions with Henry’s existing database
- Grafana can query the new database

### Phase 2: Streaming Revalidation On Shared ClickHouse
Goal: prove the live streaming path still works after the ClickHouse cutover.

Implementation:
- Re-run the end-to-end streaming shakedown using the established working pattern:
  - producer from your laptop
  - Spark consumer on EC2
  - shared ClickHouse as sink
- Use a fresh checkpoint root for the first validation run after the ClickHouse/config change.
- Verify all three sink surfaces:
  - ClickHouse `raw_ohlcv_1m`
  - ClickHouse `pipeline_metrics`
  - S3 Silver parquet
- Record the exact validation queries and expected outputs in repo docs so teammates can repeat the same checks.

Acceptance criteria:
- fresh live rows appear in shared ClickHouse `raw_ohlcv_1m`
- `pipeline_metrics` records `silver_row_count` and `ingestion_lag_seconds`
- fresh parquet appears under the stream Silver prefix in S3
- no new runtime errors from the ClickHouse cutover

### Phase 3: Finance Dashboard Upgrade On Self-Hosted Grafana
Goal: add a presentation-quality analytics dashboard while keeping the current health dashboard intact.

Implementation:
- Keep `Pipeline Shakedown Overview` as the operational/health dashboard.
- Add a second Grafana dashboard for finance-style analytics.
- Build the analytics dashboard against the shared ClickHouse database, not against raw S3.
- Use query-side aggregation and Grafana variables rather than changing ingestion behavior.
- Start with these dashboard capabilities:
  - symbol dropdown
  - timeframe dropdown with at least `1m` and `5m`
  - candlestick or OHLC-style time-series visualization
  - EMA overlays
  - VWAP overlay if feasible from available data
  - rolling volatility / volume / freshness stats
- Use ClickHouse SQL queries or views to support the dashboard. Do not push aggregation logic into the producer.
- If panel support for candlesticks is awkward in the current Grafana setup, define a fallback visualization shape in advance:
  - OHLC table-backed time series or split line charts first
  - candlestick panel second if plugin support is confirmed

Interfaces / data decisions:
- The ingestion contract stays as 1-minute candles.
- Timeframe switching is done in dashboard query logic or ClickHouse query/view logic.
- Finance dashboard and health dashboard remain separate dashboards.

Acceptance criteria:
- self-hosted Grafana shows both a health dashboard and a finance dashboard
- symbol switching works
- timeframe switching works for the supported intervals
- finance panels are backed by the shared ClickHouse database, not ad hoc files or separate local state

### Phase 4: Multi-Symbol Expansion
Goal: expand from the current validation set to a curated market set without changing the producer architecture.

Implementation:
- Keep one producer implementation.
- Expand the tracked symbol list in the existing producer configuration.
- Start with the curated top-10 set from your note:
  - BTCUSDT
  - ETHUSDT
  - BNBUSDT
  - SOLUSDT
  - XRPUSDT
  - ADAUSDT
  - DOGEUSDT
  - AVAXUSDT
  - LINKUSDT
  - DOTUSDT
- Re-run the live stream validation after the symbol expansion.
- Watch for:
  - Kinesis shard throughput pressure
  - Spark micro-batch stability
  - ClickHouse insert stability
  - dashboard responsiveness
- Only expand further if the top-10 set remains stable.

Acceptance criteria:
- the expanded symbol list streams successfully into Kinesis
- the Spark consumer continues writing to ClickHouse and S3 Silver without instability
- the dashboard can switch across the expanded symbol set cleanly

### Phase 5: Deferred Decisions
Goal: explicitly postpone non-blocking integration work until the core path is stable.

Grafana Cloud:
- Defer migration planning until after Phases 1–4 succeed.
- At that point, evaluate:
  - datasource reachability from Grafana Cloud to shared ClickHouse
  - whether dashboard JSON import is sufficient
  - whether dashboard provisioning should remain repo-managed or move to manual cloud management

S3 bucket alignment:
- Defer bucket unification until it becomes operationally necessary.
- Treat `is459-crypto-datalake` and `is459-crypto-raw-data` as both usable, provided IAM access exists.
- Only plan bucket reorganization if a teammate workflow or ClickHouse read path truly depends on it.

## Test Plan
- Phase 1:
  - create database and apply DDL successfully on shared ClickHouse
  - verify direct reads from the new database
- Phase 2:
  - run producer + consumer
  - verify fresh rows in `raw_ohlcv_1m`
  - verify stream metrics in `pipeline_metrics`
  - verify Silver parquet in S3
- Phase 3:
  - validate dashboard queries against the new shared database
  - verify finance dashboard panels render with real streaming data
- Phase 4:
  - re-run stream with the curated top-10 symbol set
  - confirm Kinesis, Spark, ClickHouse, and Grafana remain stable
- Regression checks throughout:
  - old health dashboard still works
  - checkpoint incompatibility is handled by using a fresh checkpoint root when query shape changes
  - no collision with Henry’s existing ClickHouse database

## Assumptions And Defaults
- Your separate database on the shared ClickHouse server is the default integration model.
- Self-hosted Grafana remains the active frontend during the core integration work.
- Grafana Cloud is explicitly out of scope for the first implementation pass.
- S3 bucket alignment is not required for the initial shared ClickHouse + dashboard integration.
- Existing table contracts and streaming semantics remain unchanged unless the shared ClickHouse server forces a compatibility adjustment.
