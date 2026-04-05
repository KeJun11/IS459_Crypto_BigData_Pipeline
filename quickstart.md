# Streaming Pipeline Quickstart

This quickstart covers the live streaming path only:

`Binance WebSocket -> stream_producer.py -> Kinesis -> stream_to_silver.py -> ClickHouse + S3 Silver -> Grafana`

Use this when you want to bring the streaming pipeline up quickly and verify that it is working end to end.

## Architecture

- `stream_producer.py` publishes closed 1-minute Binance candles into AWS Kinesis.
- `stream_to_silver.py` is the Spark Structured Streaming consumer.
- The consumer runs on the EC2 Docker stack, not on EMR.
- The producer can run from your laptop.
- Firehose archives the raw stream to S3 Bronze automatically.
- The Spark consumer writes:
  - cleaned rows to ClickHouse `raw_ohlcv_1m`
  - streaming metrics to ClickHouse `pipeline_metrics`
  - cleaned parquet to `s3://is459-crypto-datalake/silver/stream/`

## Important Constraint

Do not run the Binance WebSocket producer from the US EC2 instance.

Binance rejects the EC2 WebSocket connection with HTTP `451` from the current AWS region. The working setup is:

- `EC2`: run the Spark consumer
- `Your computer`: run the Binance producer

Kinesis is the shared boundary, so this is still a valid end-to-end streaming run.

## Prerequisites

Before starting, make sure:

- AWS infrastructure is already provisioned
- the Kinesis Data Stream exists
- Firehose is writing the raw archive to S3 Bronze
- the EC2 Docker stack is available
- your EC2 repo copy has the latest code
- your local machine can access Binance WebSocket and AWS credentials

## Required Runtime Settings

These should exist in the EC2 `.env`:

```env
AWS_DEFAULT_REGION=us-east-1
KINESIS_STREAM_NAME=crypto-ohlcv-1m
SHARED_CLICKHOUSE_HOST=<shared-clickhouse-host>
SHARED_CLICKHOUSE_PORT=8123
SHARED_CLICKHOUSE_DATABASE=is459_streaming_kj
STREAM_CHECKPOINT_ROOT=s3a://is459-crypto-datalake/checkpoints/stream_to_silver
STREAM_SILVER_OUTPUT=s3a://is459-crypto-datalake/silver/stream
STREAM_TRIGGER_PROCESSING_TIME=30 seconds
STREAM_INITIAL_POSITION=latest
STREAM_KINESIS_FORMAT=aws-kinesis
STREAM_KINESIS_ENDPOINT_URL=https://kinesis.us-east-1.amazonaws.com
STREAM_EXTRA_JARS=https://awslabs-code-us-east-1.s3.amazonaws.com/spark-sql-kinesis-connector/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar
STREAM_SPARK_MASTER=spark://spark-master:7077
STREAM_SPARK_CONTAINER=crypto-spark-master
```

Notes:

- `SHARED_CLICKHOUSE_HOST` is the main source of truth after the shared ClickHouse cutover.
- `STREAM_CLICKHOUSE_URL` is now optional and only needed if you want to override the shared-host derived URL explicitly.
- If you make a breaking change to the streaming query, use a new checkpoint root instead of reusing the old one.

## Step 1: Start the EC2 Stack

SSH into EC2:

```bash
ssh -i "is459-emr.pem" ec2-user@<ec2-public-dns>
```

Go to the repo and start the stack:

```bash
cd /opt/is459-crypto-bigdata-pipeline
docker compose up -d
docker compose ps
```

You want at least these containers up:

- `crypto-clickhouse`
- `crypto-grafana`
- `crypto-spark-master`
- `crypto-spark-worker`

## Step 2: Start the Spark Consumer on EC2

On EC2:

```bash
cd /opt/is459-crypto-bigdata-pipeline
uv run scripts/run_stream_to_silver.py
```

If you recently changed the stream query and hit checkpoint-related failures, start with a new checkpoint root:

```bash
uv run scripts/run_stream_to_silver.py -- --checkpoint-root s3a://is459-crypto-datalake/checkpoints/stream_to_silver_v2
```

Leave this process running.

## Step 3: Start the Producer From Your Computer

From your laptop, in the repo root:

```powershell
uv run -m src.ingestion.stream_producer --max-records 15 --log-level INFO
```

Expected producer logs look like:

- `websocket connected`
- `published symbol=BTCUSDT ...`
- `published symbol=ETHUSDT ...`
- `published symbol=SOLUSDT ...`

This proves candles are being published into Kinesis.

## Step 4: Validate the Stream on EC2

SSH into EC2 and open ClickHouse:

```bash
docker exec -it crypto-clickhouse clickhouse-client
```

Run these queries.

Fresh rows in the raw stream sink:

```sql
SELECT symbol, max(open_time) AS latest_open_time, count() AS total_rows
FROM crypto.raw_ohlcv_1m
GROUP BY symbol
ORDER BY symbol;
```

Streaming metrics summary:

```sql
SELECT max(metric_time) AS latest_metric_time, count() AS metric_rows
FROM crypto.pipeline_metrics
WHERE job_name = 'stream_to_silver';
```

Recent streaming metrics:

```sql
SELECT metric_time, metric_name, metric_value, job_name, details
FROM crypto.pipeline_metrics
WHERE job_name = 'stream_to_silver'
ORDER BY metric_time DESC
LIMIT 10;
```

Exit:

```sql
exit
```

Check that Silver parquet was written:

```bash
aws s3 ls s3://is459-crypto-datalake/silver/stream/ --recursive | tail -n 20
```

## Step 5: Check Grafana

Open Grafana on EC2:

```text
http://<ec2-public-dns>:3000
```

Use:

- username: `admin`
- password: `admin`

Open both dashboards:

- `Pipeline Shakedown Overview` for health checks
- `Streaming Market Analytics` for the finance-style symbol/timeframe view

The health dashboard panels that should reflect the stream are:

- `Latest Raw Candle By Symbol`
- `Raw Rows Per Minute (Last 60m)`
- `Recent Pipeline Metrics`
- `Recent Stream Metrics`

The analytics dashboard should give you:

- a symbol dropdown
- a timeframe dropdown for `1m` and `5m`
- a candlestick view when the Grafana panel renders correctly
- price and trend overlays
- session VWAP
- bucketed volume
- return and rolling volatility
- a recent OHLC fallback table if you prefer the tabular view

Set the time range to `Last 24 hours` if the dashboard looks empty.

## What “Success” Looks Like

The streaming shakedown is successful when all of these are true:

- producer logs show successful Kinesis publishes
- `stream_to_silver.py` stays running on EC2
- `crypto.raw_ohlcv_1m` shows fresh `latest_open_time` values
- `crypto.pipeline_metrics` contains `silver_row_count` and `ingestion_lag_seconds` for `job_name = 'stream_to_silver'`
- `s3://is459-crypto-datalake/silver/stream/` contains fresh parquet files
- Grafana shows recent raw timestamps and stream metrics

## Expected Lag

An `ingestion_lag_seconds` value around `90-120 seconds` is plausible in this project.

That is mostly due to:

- Binance closed 1-minute candle semantics
- lag being measured from `open_time`
- Spark Structured Streaming micro-batches
- the configured trigger interval of `30 seconds`

This is not mainly a Kinesis limitation.

## Troubleshooting

### `HTTP 451` from Binance

Cause:
- Binance rejects WebSocket access from the current US EC2 region

Fix:
- run the producer from your laptop, not from EC2

### `DATA_SOURCE_NOT_FOUND: kinesis`

Cause:
- the Spark Kinesis connector jar is missing

Fix:
- set `STREAM_EXTRA_JARS` to the AWS connector jar URL shown above

### `kinesis.endpointUrl is not specified`

Cause:
- the AWS Kinesis connector needs an explicit endpoint

Fix:
- set `STREAM_KINESIS_ENDPOINT_URL=https://kinesis.us-east-1.amazonaws.com`

### Spark internal error after restarting the consumer

Cause:
- old checkpoint metadata is incompatible with the new query plan

Fix:
- run with a new checkpoint root such as `stream_to_silver_v2`

### Grafana shows no stream data

Check these in order:

1. producer logs show successful Kinesis publishes
2. ClickHouse queries show fresh `raw_ohlcv_1m` rows
3. `pipeline_metrics` has `stream_to_silver` rows
4. Silver parquet exists in S3
5. Grafana time range is wide enough
6. Grafana is pointed at the correct ClickHouse instance

## Useful Commands Summary

Consumer on EC2:

```bash
uv run scripts/run_stream_to_silver.py
```

Consumer on EC2 with fresh checkpoint:

```bash
uv run scripts/run_stream_to_silver.py --checkpoint-root s3a://is459-crypto-datalake/checkpoints/stream_to_silver_v2
```

Producer on your laptop:

```powershell
uv run -m src.ingestion.stream_producer --max-records 15 --log-level INFO
```

ClickHouse interactive shell on EC2:

```bash
docker exec -it crypto-clickhouse clickhouse-client
```
