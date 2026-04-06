from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional in Spark runtime images
    def load_dotenv() -> bool:
        return False
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    avg as spark_avg,
    col,
    current_timestamp,
    date_format,
    expr,
    from_json,
    lit,
    max as spark_max,
    month,
    to_timestamp,
    year,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


load_dotenv()


STREAM_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("open", DoubleType(), nullable=False),
        StructField("high", DoubleType(), nullable=False),
        StructField("low", DoubleType(), nullable=False),
        StructField("close", DoubleType(), nullable=False),
        StructField("volume", DoubleType(), nullable=False),
        StructField("open_time", LongType(), nullable=False),
        StructField("close_time", LongType(), nullable=False),
        StructField("is_closed", BooleanType(), nullable=False),
    ]
)


DEFAULT_STREAM_NAME = "crypto-ohlcv-1m"
DEFAULT_REGION = "us-east-1"
DEFAULT_CHECKPOINT_ROOT = "s3a://is459-crypto-datalake/checkpoints/stream_to_silver"
DEFAULT_SILVER_OUTPUT = "s3a://is459-crypto-datalake/silver/stream"
DEFAULT_TRIGGER_PROCESSING_TIME = "30 seconds"
DEFAULT_CLICKHOUSE_URL = "http://clickhouse:8123"
DEFAULT_CLICKHOUSE_DATABASE = "crypto"
DEFAULT_CLICKHOUSE_TABLE = "raw_ohlcv_1m"
DEFAULT_METRICS_TABLE = "pipeline_metrics"
DEFAULT_JOB_NAME = "stream_to_silver"
DEFAULT_KINESIS_FORMAT = "aws-kinesis"


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def env_first(names: list[str], default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def build_default_clickhouse_url() -> str:
    host = env_first(["STREAM_CLICKHOUSE_HOST", "SHARED_CLICKHOUSE_HOST", "AIRFLOW_CLICKHOUSE_HOST"], "")
    if not host:
        return DEFAULT_CLICKHOUSE_URL
    port = env_first(
        ["STREAM_CLICKHOUSE_PORT", "SHARED_CLICKHOUSE_PORT", "AIRFLOW_CLICKHOUSE_PORT"],
        "8123",
    )
    return f"http://{host}:{port}"


def build_default_kinesis_endpoint_url(region: str) -> str:
    return f"https://kinesis.{region}.amazonaws.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume Binance candle events from Kinesis and write cleaned rows to Silver sinks."
    )
    parser.add_argument(
        "--stream-name",
        default=env_or_default("KINESIS_STREAM_NAME", DEFAULT_STREAM_NAME),
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", DEFAULT_REGION)),
    )
    parser.add_argument(
        "--initial-position",
        default=env_or_default("STREAM_INITIAL_POSITION", "latest"),
        choices=["latest", "trim_horizon"],
    )
    parser.add_argument(
        "--checkpoint-root",
        default=env_or_default("STREAM_CHECKPOINT_ROOT", DEFAULT_CHECKPOINT_ROOT),
    )
    parser.add_argument(
        "--silver-output",
        default=env_or_default("STREAM_SILVER_OUTPUT", DEFAULT_SILVER_OUTPUT),
    )
    parser.add_argument(
        "--trigger-processing-time",
        default=env_or_default("STREAM_TRIGGER_PROCESSING_TIME", DEFAULT_TRIGGER_PROCESSING_TIME),
    )
    parser.add_argument(
        "--clickhouse-url",
        default=env_first(
            ["STREAM_CLICKHOUSE_URL", "AIRFLOW_CLICKHOUSE_URL"],
            build_default_clickhouse_url(),
        ),
    )
    parser.add_argument(
        "--clickhouse-database",
        default=env_first(
            ["STREAM_CLICKHOUSE_DATABASE", "AIRFLOW_CLICKHOUSE_DATABASE", "SHARED_CLICKHOUSE_DATABASE"],
            DEFAULT_CLICKHOUSE_DATABASE,
        ),
    )
    parser.add_argument(
        "--clickhouse-table",
        default=env_or_default("STREAM_CLICKHOUSE_TABLE", DEFAULT_CLICKHOUSE_TABLE),
    )
    parser.add_argument(
        "--metrics-table",
        default=env_or_default("STREAM_METRICS_TABLE", DEFAULT_METRICS_TABLE),
    )
    parser.add_argument(
        "--job-name",
        default=env_or_default("STREAM_JOB_NAME", DEFAULT_JOB_NAME),
    )
    parser.add_argument(
        "--kinesis-format",
        default=env_or_default("STREAM_KINESIS_FORMAT", DEFAULT_KINESIS_FORMAT),
        choices=["aws-kinesis", "kinesis"],
    )
    parser.add_argument(
        "--kinesis-endpoint-url",
        default=os.getenv("STREAM_KINESIS_ENDPOINT_URL"),
    )
    parser.add_argument("--await-termination-seconds", type=int)
    return parser.parse_args()


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_parsed_stream_df(raw_stream_df: DataFrame) -> DataFrame:
    return (
        raw_stream_df.select(
            col("data").cast("string").alias("json_payload"),
            col("approximateArrivalTimestamp").alias("kinesis_arrival_timestamp"),
        )
        .withColumn("record", from_json(col("json_payload"), STREAM_SCHEMA))
    )


def build_record_validity_condition() -> Column:
    record = col("record")
    return (
        record.isNotNull()
        & record.symbol.isNotNull()
        & record.open.isNotNull()
        & record.high.isNotNull()
        & record.low.isNotNull()
        & record.close.isNotNull()
        & record.volume.isNotNull()
        & record.open_time.isNotNull()
        & record.close_time.isNotNull()
        & record.is_closed.isNotNull()
        & (record.low <= record.high)
        & (record.open_time <= record.close_time)
        & (record.volume >= lit(0.0))
    )


def build_invalid_stream_df(parsed_stream_df: DataFrame) -> DataFrame:
    return parsed_stream_df.filter(~build_record_validity_condition()).select(
        "json_payload",
        "kinesis_arrival_timestamp",
    )


def build_valid_stream_df(parsed_stream_df: DataFrame) -> DataFrame:
    return (
        parsed_stream_df.filter(build_record_validity_condition())
        .select("record.*", "kinesis_arrival_timestamp")
        .withColumn("source", lit("binance_websocket"))
        .withColumn("processed_at", current_timestamp())
        .withColumn("open_time_ts", to_timestamp((col("open_time") / lit(1000)).cast("double")))
        .withColumn("event_time_ts", to_timestamp((col("close_time") / lit(1000)).cast("double")))
    )


def build_cleaned_stream_df(valid_stream_df: DataFrame, streaming_mode: bool = True) -> DataFrame:
    if streaming_mode:
        return valid_stream_df.withWatermark("open_time_ts", "5 minutes").dropDuplicates(["symbol", "open_time"])
    return valid_stream_df.dropDuplicates(["symbol", "open_time"])


def clickhouse_insert_json_each_row(url: str, query: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    payload = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows).encode("utf-8")
    request = urllib.request.Request(
        url=f"{url}/?query={urllib.parse.quote(query)}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse insert failed ({exc.code}): {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"ClickHouse insert failed: {exc}") from exc


def insert_dataframe_into_clickhouse(
    batch_df: DataFrame,
    clickhouse_url: str,
    database: str,
    table: str,
) -> None:
    rows = [
        {
            "symbol": row["symbol"],
            "open_time": row["open_time_str"],
            "close_time": row["close_time_str"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "is_closed": row["is_closed_int"],
            "source": row["source"],
        }
        for row in batch_df.select(
            "symbol",
            "open_time_str",
            "close_time_str",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "is_closed_int",
            "source",
        ).toLocalIterator()
    ]
    query = f"INSERT INTO {database}.{table} FORMAT JSONEachRow"
    clickhouse_insert_json_each_row(clickhouse_url, query, rows)


def insert_metric_rows(
    clickhouse_url: str,
    database: str,
    table: str,
    rows: list[dict[str, Any]],
) -> None:
    query = f"INSERT INTO {database}.{table} FORMAT JSONEachRow"
    clickhouse_insert_json_each_row(clickhouse_url, query, rows)


def build_metric_row(
    metric_name: str,
    metric_value: float,
    unit: str,
    job_name: str,
    run_id: str,
    details: str = "",
    symbol: str | None = None,
) -> dict[str, Any]:
    return {
        "metric_time": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "component": "spark_stream",
        "metric_name": metric_name,
        "symbol": symbol,
        "metric_value": float(metric_value),
        "unit": unit,
        "job_name": job_name,
        "run_id": run_id,
        "details": details,
    }


def write_cleaned_batch(
    batch_df: DataFrame,
    batch_id: int,
    silver_output: str,
    clickhouse_url: str,
    database: str,
    table: str,
    metrics_table: str,
    job_name: str,
    run_id: str,
) -> None:
    if batch_df.isEmpty():
        return

    batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    row_count = batch_df.count()

    silver_batch_df = (
        batch_df.withColumn("year", year(col("open_time_ts")))
        .withColumn("month", month(col("open_time_ts")))
    )
    (
        silver_batch_df.write.mode("append")
        .partitionBy("symbol", "year", "month")
        .parquet(silver_output)
    )

    clickhouse_ready_df = (
        silver_batch_df.withColumn("open_time_str", date_format(col("open_time_ts"), "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("close_time_ts", to_timestamp((col("close_time") / lit(1000)).cast("double")))
        .withColumn("close_time_str", date_format(col("close_time_ts"), "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("is_closed_int", expr("CASE WHEN is_closed THEN 1 ELSE 0 END"))
    )
    insert_dataframe_into_clickhouse(clickhouse_ready_df, clickhouse_url, database, table)

    latency_row = (
        batch_df.withColumn(
            "processing_latency_seconds",
            expr("CAST(processed_at AS DOUBLE) - CAST(kinesis_arrival_timestamp AS DOUBLE)"),
        )
        .agg(
            spark_max("processing_latency_seconds").alias("max_processing_latency_seconds"),
            spark_avg("processing_latency_seconds").alias("avg_processing_latency_seconds"),
        )
        .collect()[0]
    )
    lag_seconds = float(latency_row["max_processing_latency_seconds"] or 0.0)
    avg_lag_seconds = float(latency_row["avg_processing_latency_seconds"] or 0.0)

    metric_rows = [
        build_metric_row(
            metric_name="silver_row_count",
            metric_value=row_count,
            unit="rows",
            job_name=job_name,
            run_id=run_id,
            details=f"batch_id={batch_id}",
        ),
        build_metric_row(
            metric_name="ingestion_lag_seconds",
            metric_value=lag_seconds,
            unit="seconds",
            job_name=job_name,
            run_id=run_id,
            details=f"batch_id={batch_id};basis=processed_at_minus_kinesis_arrival;avg_seconds={avg_lag_seconds:.6f}",
        ),
    ]
    insert_metric_rows(clickhouse_url, database, metrics_table, metric_rows)
    batch_df.unpersist()


def write_invalid_metrics_batch(
    batch_df: DataFrame,
    batch_id: int,
    clickhouse_url: str,
    database: str,
    metrics_table: str,
    job_name: str,
    run_id: str,
) -> None:
    count_value = batch_df.count()
    if count_value == 0:
        return

    metric_rows = [
        build_metric_row(
            metric_name="bad_record_count",
            metric_value=count_value,
            unit="rows",
            job_name=job_name,
            run_id=run_id,
            details=f"batch_id={batch_id}",
        )
    ]
    insert_metric_rows(clickhouse_url, database, metrics_table, metric_rows)


def start_cleaned_query(
    cleaned_stream_df: DataFrame,
    args: argparse.Namespace,
    run_id: str,
) -> StreamingQuery:
    return (
        cleaned_stream_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_cleaned_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                silver_output=args.silver_output,
                clickhouse_url=args.clickhouse_url,
                database=args.clickhouse_database,
                table=args.clickhouse_table,
                metrics_table=args.metrics_table,
                job_name=args.job_name,
                run_id=run_id,
            )
        )
        .option("checkpointLocation", f"{args.checkpoint_root}/cleaned")
        .trigger(processingTime=args.trigger_processing_time)
        .start()
    )


def start_invalid_metrics_query(
    invalid_stream_df: DataFrame,
    args: argparse.Namespace,
    run_id: str,
) -> StreamingQuery:
    return (
        invalid_stream_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_invalid_metrics_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                clickhouse_url=args.clickhouse_url,
                database=args.clickhouse_database,
                metrics_table=args.metrics_table,
                job_name=args.job_name,
                run_id=run_id,
            )
        )
        .option("checkpointLocation", f"{args.checkpoint_root}/invalid_metrics")
        .trigger(processingTime=args.trigger_processing_time)
        .start()
    )


def main() -> None:
    args = parse_args()
    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    spark = build_spark_session(args.job_name)
    spark.sparkContext.setLogLevel("WARN")

    raw_stream_df = (
        spark.readStream.format(args.kinesis_format)
        .options(
            **(
                {
                    "kinesis.streamName": args.stream_name,
                    "kinesis.region": args.region,
                    "kinesis.endpointUrl": args.kinesis_endpoint_url
                    or build_default_kinesis_endpoint_url(args.region),
                    "kinesis.startingposition": args.initial_position.upper(),
                }
                if args.kinesis_format == "aws-kinesis"
                else {
                    "streamName": args.stream_name,
                    "region": args.region,
                    "initialPosition": args.initial_position,
                }
            )
        )
        .load()
    )

    parsed_stream_df = build_parsed_stream_df(raw_stream_df)
    invalid_stream_df = build_invalid_stream_df(parsed_stream_df)
    valid_stream_df = build_valid_stream_df(parsed_stream_df)
    cleaned_stream_df = build_cleaned_stream_df(valid_stream_df, streaming_mode=True)

    queries = [
        start_cleaned_query(cleaned_stream_df, args, run_id),
        start_invalid_metrics_query(invalid_stream_df, args, run_id),
    ]

    try:
        if args.await_termination_seconds:
            for query in queries:
                query.awaitTermination(args.await_termination_seconds)
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
