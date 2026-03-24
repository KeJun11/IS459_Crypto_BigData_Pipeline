from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    date_format,
    expr,
    lit,
    month,
    row_number,
    to_timestamp,
    year,
)

from src.jobs.clickhouse_client import ClickHouseConnection, insert_json_each_row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch Spark job for Bronze to Silver validation, deduplication, and raw ClickHouse upsert."
    )
    parser.add_argument("--input-root", default="data/bronze/rest")
    parser.add_argument("--silver-output", default="data/silver/rest")
    parser.add_argument("--start-date", help="Optional UTC date filter in YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="Optional UTC date filter in YYYY-MM-DD format.")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--clickhouse-docker-container", default="")
    parser.add_argument("--clickhouse-database", default="crypto")
    parser.add_argument("--clickhouse-table", default="raw_ohlcv_1m")
    parser.add_argument("--metrics-table", default="pipeline_metrics")
    parser.add_argument("--job-name", default="bronze_to_silver_batch")
    parser.add_argument("--skip-clickhouse", action="store_true")
    return parser.parse_args()


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def ensure_optional_columns(bronze_df: DataFrame) -> DataFrame:
    df = bronze_df
    if "source" not in df.columns:
        df = df.withColumn("source", lit("binance_rest"))
    if "ingested_at" not in df.columns:
        current_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        df = df.withColumn("ingested_at", lit(current_ms))
    return df


def build_typed_bronze_df(bronze_df: DataFrame) -> DataFrame:
    bronze_df = ensure_optional_columns(bronze_df)
    return (
        bronze_df.select(
            col("symbol").cast("string").alias("symbol"),
            col("open").cast("double").alias("open"),
            col("high").cast("double").alias("high"),
            col("low").cast("double").alias("low"),
            col("close").cast("double").alias("close"),
            col("volume").cast("double").alias("volume"),
            col("open_time").cast("long").alias("open_time"),
            col("close_time").cast("long").alias("close_time"),
            col("is_closed").cast("boolean").alias("is_closed"),
            coalesce(col("source").cast("string"), lit("binance_rest")).alias("source"),
            col("ingested_at").cast("long").alias("ingested_at"),
        )
        .withColumn("open_time_ts", to_timestamp((col("open_time") / lit(1000)).cast("double")))
        .withColumn("close_time_ts", to_timestamp((col("close_time") / lit(1000)).cast("double")))
    )


def apply_date_filter(df: DataFrame, start_date: str | None, end_date: str | None) -> DataFrame:
    filtered_df = df
    if start_date:
        filtered_df = filtered_df.filter(col("open_time_ts") >= lit(f"{start_date} 00:00:00"))
    if end_date:
        filtered_df = filtered_df.filter(col("open_time_ts") < lit(f"{end_date} 23:59:59.999"))
    return filtered_df


def build_record_validity_condition() -> Column:
    return (
        col("symbol").isNotNull()
        & col("open").isNotNull()
        & col("high").isNotNull()
        & col("low").isNotNull()
        & col("close").isNotNull()
        & col("volume").isNotNull()
        & col("open_time").isNotNull()
        & col("close_time").isNotNull()
        & col("is_closed").isNotNull()
        & col("open_time_ts").isNotNull()
        & col("close_time_ts").isNotNull()
        & (col("is_closed") == lit(True))
        & (col("low") <= col("high"))
        & (col("open_time") <= col("close_time"))
        & (col("volume") >= lit(0.0))
    )


def build_invalid_bronze_df(typed_bronze_df: DataFrame) -> DataFrame:
    return typed_bronze_df.filter(~build_record_validity_condition())


def build_valid_bronze_df(typed_bronze_df: DataFrame) -> DataFrame:
    return typed_bronze_df.filter(build_record_validity_condition())


def build_cleaned_silver_df(valid_bronze_df: DataFrame) -> DataFrame:
    latest_row_window = Window.partitionBy("symbol", "open_time").orderBy(
        col("ingested_at").desc_nulls_last(),
        col("close_time").desc(),
    )
    return (
        valid_bronze_df.withColumn("row_number", row_number().over(latest_row_window))
        .filter(col("row_number") == 1)
        .drop("row_number")
        .withColumn("processed_at", current_timestamp())
    )


def build_metric_row(
    metric_name: str,
    metric_value: float,
    unit: str,
    job_name: str,
    run_id: str,
    details: str = "",
) -> dict:
    return {
        "metric_time": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "component": "spark_batch",
        "metric_name": metric_name,
        "symbol": None,
        "metric_value": float(metric_value),
        "unit": unit,
        "job_name": job_name,
        "run_id": run_id,
        "details": details,
    }


def build_clickhouse_raw_rows(cleaned_silver_df: DataFrame) -> list[dict]:
    clickhouse_ready_df = (
        cleaned_silver_df.withColumn("open_time_str", date_format(col("open_time_ts"), "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("close_time_str", date_format(col("close_time_ts"), "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("is_closed_int", expr("CASE WHEN is_closed THEN 1 ELSE 0 END"))
    )
    return [
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
        for row in clickhouse_ready_df.select(
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


def write_silver_parquet(cleaned_silver_df: DataFrame, output_root: str) -> None:
    silver_output_df = (
        cleaned_silver_df.withColumn("year", year(col("open_time_ts")))
        .withColumn("month", month(col("open_time_ts")))
    )
    (
        silver_output_df.write.mode("append")
        .partitionBy("symbol", "year", "month")
        .parquet(output_root)
    )


def run_job(args: argparse.Namespace) -> dict[str, int]:
    spark = build_spark_session(args.job_name)
    spark.sparkContext.setLogLevel("WARN")
    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    bronze_df = spark.read.parquet(args.input_root)
    typed_bronze_df = apply_date_filter(build_typed_bronze_df(bronze_df), args.start_date, args.end_date)
    invalid_bronze_df = build_invalid_bronze_df(typed_bronze_df)
    valid_bronze_df = build_valid_bronze_df(typed_bronze_df)
    cleaned_silver_df = build_cleaned_silver_df(valid_bronze_df)

    bronze_row_count = typed_bronze_df.count()
    invalid_row_count = invalid_bronze_df.count()
    silver_row_count = cleaned_silver_df.count()

    write_silver_parquet(cleaned_silver_df, args.silver_output)

    if not args.skip_clickhouse:
        connection = ClickHouseConnection(
            url=args.clickhouse_url,
            docker_container=args.clickhouse_docker_container,
        )
        raw_rows = build_clickhouse_raw_rows(cleaned_silver_df)
        insert_json_each_row(connection, args.clickhouse_database, args.clickhouse_table, raw_rows)
        metric_rows = [
            build_metric_row("bronze_input_row_count", bronze_row_count, "rows", args.job_name, run_id),
            build_metric_row("silver_row_count", silver_row_count, "rows", args.job_name, run_id),
            build_metric_row("bad_record_count", invalid_row_count, "rows", args.job_name, run_id),
        ]
        insert_json_each_row(connection, args.clickhouse_database, args.metrics_table, metric_rows)

    spark.stop()
    return {
        "bronze_row_count": bronze_row_count,
        "invalid_row_count": invalid_row_count,
        "silver_row_count": silver_row_count,
    }


def main() -> None:
    result = run_job(parse_args())
    print(
        "bronze_to_silver_batch completed "
        f"(bronze={result['bronze_row_count']}, invalid={result['invalid_row_count']}, silver={result['silver_row_count']})"
    )


if __name__ == "__main__":
    main()
