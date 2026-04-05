## Streaming Integration Notes

This note captures the practical integration work needed to merge the end-to-end streaming pipeline with my teammates' setup.

The goal is not to redesign the pipeline. The goal is to plug the existing streaming path into the shared team environment cleanly:

`Binance WebSocket -> Kinesis -> Spark consumer -> ClickHouse -> Grafana`

## 1. Shared ClickHouse Integration

### Current situation

- Henry already has a ClickHouse instance on EC2.
- His existing schemas live in a different ClickHouse database, so table-name clashes should not be a problem.
- My streaming pipeline currently writes to these logical tables:
  - `raw_ohlcv_1m`
  - `pipeline_metrics`
  - `agg_ohlcv_1h`
  - `agg_ohlcv_1d`
  - `technical_features`

### Integration direction

- Use the shared ClickHouse server.
- Create a separate database for my pipeline rather than mixing directly into Henry's existing database.
- Keep my schema definitions isolated so the integration is reversible and low-risk.

### What needs to be done

- Create a dedicated database for my pipeline on Henry's ClickHouse instance.
- Apply my ClickHouse DDLs into that database.
- Update my runtime config so:
  - the Spark streaming consumer writes into the shared ClickHouse host
  - Grafana reads from the same ClickHouse database
- Confirm that the schema and table engines are unchanged from my current working setup.

### Open assumption

- I am assuming I have permission to create a new database and tables on the shared ClickHouse instance.

## 2. Grafana Dashboard Upgrade

### Current situation

- The current Grafana dashboard is mainly table-based.
- It is useful for validation and health checks, but it does not yet look like a finance dashboard.

### Desired upgrade

The next dashboard should look more like a live market analytics dashboard instead of a technical validation board.

### Candidate visuals

- 1-minute candlestick chart fed by streamed data
- EMA overlays
- VWAP overlay
- rolling volatility
- ATR
- volume panels
- latest price / freshness / ingestion lag stat panels
- symbol dropdown
- timeframe dropdown such as `1m`, `5m`, maybe `15m`

### Important design note

- I do **not** need multiple producer implementations.
- The existing producer already supports multiple symbols in one process.
- If I want more symbols, I should expand the tracked symbol list, not build more producer scripts.
- Timeframe switching should happen in ClickHouse query logic or Grafana query variables, not at ingestion time.

### Integration direction

- Keep the current `Pipeline Shakedown Overview` dashboard as the operational/health dashboard.
- Add a second dashboard focused on finance-style analytics and presentation.

## 3. Multi-Symbol Expansion

### Current situation

- The current streaming setup already supports multiple symbols.
- The original tracked set was small for shakedown and validation.

### Desired expansion

- Expand the live symbol set so the dashboard can switch across a broader market view.

### Candidate symbols

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

### Integration direction

- Start with a curated top-10 symbol list.
- Confirm the additional throughput is still fine for:
  - 1 Kinesis shard
  - current Spark consumer setup
  - current ClickHouse EC2 host

## 4. Grafana Cloud Decision

### Current situation

- My teammate already uses Grafana Cloud.
- I am not yet sure whether we should keep using the self-hosted Grafana on EC2 or move dashboarding to Grafana Cloud.

### Actual integration question

This is not a dashboard-design task. It is a deployment decision:

- Option A: keep self-hosted Grafana on EC2
- Option B: switch to Grafana Cloud as the shared dashboard frontend

### What needs to be clarified

- whether Grafana Cloud can reach the shared ClickHouse instance
- whether dashboards will be provisioned from repo or edited manually in the cloud
- who owns the final datasource configuration
- whether both dashboards need to live in the same Grafana workspace

### Integration direction

- Treat this as a hosting/infrastructure decision separate from the dashboard feature work.

## 5. S3 Bucket Alignment

### Current situation

- Henry's ClickHouse is already set up against my other teammate's bucket: `is459-crypto-raw-data`
- My current data lives in: `is459-crypto-datalake`
- Both buckets are in the same AWS account

### What is technically true

- ClickHouse can read from multiple S3 buckets.
- The buckets do not need to be the same.
- The key requirement is IAM/credential access to both.

### Integration direction

- This is lower priority than ClickHouse/database integration and dashboard integration.
- If needed, the shared ClickHouse can read from both buckets temporarily.
- Only unify bucket conventions if it materially simplifies teammate workflows.

## 6. Recommended Order

The safest integration order is:

1. Connect to Henry's shared ClickHouse server
2. Create a separate database for my schemas
3. Repoint my streaming consumer and Grafana datasource to that database
4. Verify the current end-to-end stream still works there
5. Improve the dashboard from table-based panels to finance-style panels
6. Expand the tracked symbol set
7. Decide whether to stay on EC2 Grafana or move to Grafana Cloud
8. Revisit S3 bucket alignment only if it becomes necessary

## 7. Main Risks

- Shared ClickHouse permissions may not allow database creation
- Grafana Cloud may not be able to reach the shared ClickHouse endpoint without extra networking work
- Mixed bucket usage may confuse ownership and data lineage if not documented clearly
- Expanding symbols too early may add noise before the dashboard and shared database setup are stable
