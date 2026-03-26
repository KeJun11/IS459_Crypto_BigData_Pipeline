# Shakedown With Initial Kaggle Bronze Conversion

## Summary
Use a three-stage shakedown instead of jumping straight into Phase 8. First, run a one-time normalization step that converts the Kaggle CSV already stored in S3 into Bronze parquet in the schema your batch pipeline expects. Second, use that Bronze parquet for a fast local operator shakedown. Third, repeat the same workflow on EC2 + AWS to validate the deployed topology.

This keeps the Kaggle dataset as seed data only. It does not become the long-term Bronze format or a permanent DAG input.

## Implementation Changes
- Add a one-time Spark conversion job for Kaggle seed data.
  - Input: Kaggle CSV files already uploaded to S3.
  - Output: partitioned Bronze parquet in a separate prefix, not mixed with the normal Binance REST Bronze prefix.
  - Recommended target prefix: `s3://<bucket>/bronze/rest/kaggle-seed/`
  - Normalize each row to the Phase 6 Bronze schema used by `bronze_to_silver_batch.py`:
    - `symbol`
    - `open`
    - `high`
    - `low`
    - `close`
    - `volume`
    - `open_time`
    - `close_time`
    - `is_closed`
    - `source`
    - `ingested_at`
  - Map `timestamp` and `close_time` from the CSV into epoch-millisecond fields.
  - Set `symbol` from the dataset context if the CSV is single-symbol.
  - Set `is_closed = true` for all converted Kaggle rows.
  - Set `source = "kaggle_csv"`.
  - Set `ingested_at` to conversion time in epoch milliseconds.
  - Partition output by `symbol`, `year`, and `month` based on `open_time`.

- Keep the Kaggle conversion outside the normal Airflow DAGs.
  - It is a manual, one-time preprocessing step.
  - `backfill_batch_reprocessing` and `daily_aggregate_refresh` should continue to start from Bronze parquet, not CSV.

- Revise the shakedown sequence.
  - Stage 0: run Kaggle CSV -> Bronze parquet conversion and verify the Bronze output exists in S3.
  - Stage 1: local operator shakedown using the converted Bronze parquet.
  - Stage 2: EC2 + AWS shakedown using the same converted Bronze parquet and the same operator workflow.
  - After that, switch the Bronze source for ongoing historical ingestion back to the real Binance REST ingestion flow.

- Keep the minimal observability work from the earlier plan.
  - Add one provisioned Grafana dashboard for raw row freshness, per-minute ingestion counts, aggregate counts, and recent pipeline metrics.
  - Add runbook steps showing how to validate the Bronze conversion output, then trigger the batch DAGs, then observe streaming.

## Interfaces / Runtime Surfaces
- Add one new manual job surface: a one-time Kaggle Bronze normalization Spark job.
- No changes to the existing batch DAG interfaces.
- No changes to the Bronze-to-Silver or Silver-to-Gold job interfaces beyond pointing them at the Kaggle-seed Bronze parquet prefix during shakedown.
- Bronze contract remains parquet-first after normalization.

## Test Plan
- Kaggle conversion validation:
  - Run the conversion on a small known CSV slice first.
  - Verify output parquet files are written under the Kaggle-seed Bronze prefix.
  - Verify the output schema matches what `bronze_to_silver_batch.py` reads.
  - Verify `symbol`, `open_time`, `close_time`, `source`, and `is_closed` are populated correctly.

- Local shakedown:
  - Point the batch workflow at the converted Kaggle Bronze parquet.
  - Trigger `backfill_batch_reprocessing` for a small date range that exists in the converted data.
  - Confirm ClickHouse row counts increase in `raw_ohlcv_1m`, `agg_ohlcv_1h`, and `agg_ohlcv_1d`.
  - Confirm Grafana shows the expected batch-side visibility.

- EC2 + AWS shakedown:
  - Reuse the same converted Bronze parquet in S3.
  - Trigger the same batch DAGs from the deployed Airflow stack.
  - Confirm the same ClickHouse validations pass on EC2.
  - Only after one successful manual EC2 run should the scheduled daily DAG be unpaused.

## Assumptions And Defaults
- The Kaggle CSV is seed data only and remains separate from the normal Binance REST Bronze path.
- The CSV layout is the single-symbol BTCUSDT format currently seen in `data/BTCUSDT/BTCUSDT.csv`, with `timestamp` and `close_time` as datetime strings.
- The one-time conversion should be implemented as a Spark job that can run locally first and on EMR second, but it is not orchestrated by Airflow.
- The normal long-term Bronze format for batch remains parquet, not CSV.
- Streaming remains unchanged: Firehose archives raw stream data to Bronze, and `stream_to_silver.py` writes cleaned parquet to Silver plus rows/metrics to ClickHouse.
