from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_format, expr, lit, max as spark_max, min as spark_min, sum as spark_sum, to_timestamp

from src.jobs.clickhouse_client import ClickHouseConnection, insert_json_each_row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch Spark job for Silver to Gold aggregate generation and ClickHouse upsert."
    )
    parser.add_argument("--silver-input", default="data/silver/rest")
    parser.add_argument("--start-date", help="Optional UTC date filter in YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="Optional UTC date filter in YYYY-MM-DD format.")
    parser.add_argument("--hourly-output", default="")
    parser.add_argument("--daily-output", default="")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--clickhouse-docker-container", default="")
    parser.add_argument("--clickhouse-database", default="crypto")
    parser.add_argument("--hourly-table", default="agg_ohlcv_1h")
    parser.add_argument("--daily-table", default="agg_ohlcv_1d")
    parser.add_argument("--metrics-table", default="pipeline_metrics")
    parser.add_argument("--job-name", default="silver_to_gold_batch")
    parser.add_argument("--skip-clickhouse", action="store_true")
    return parser.parse_args()


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_silver_df(spark: SparkSession, input_root: str) -> DataFrame:
    silver_df = spark.read.parquet(input_root)
    if "open_time_ts" not in silver_df.columns:
        silver_df = silver_df.withColumn("open_time_ts", to_timestamp((col("open_time") / lit(1000)).cast("double")))
    return silver_df


def apply_date_filter(df: DataFrame, start_date: str | None, end_date: str | None) -> DataFrame:
    filtered_df = df
    if start_date:
        filtered_df = filtered_df.filter(col("open_time_ts") >= lit(f"{start_date} 00:00:00"))
    if end_date:
        filtered_df = filtered_df.filter(col("open_time_ts") < lit(f"{end_date} 23:59:59.999"))
    return filtered_df


def build_aggregate_df(silver_df: DataFrame, bucket_expr: str, source_name: str) -> DataFrame:
    return (
        silver_df.withColumn("bucket_start", expr(bucket_expr))
        .groupBy("symbol", "bucket_start")
        .agg(
            expr("min_by(open, open_time_ts)").alias("open"),
            spark_max("high").alias("high"),
            spark_min("low").alias("low"),
            expr("max_by(close, open_time_ts)").alias("close"),
            spark_sum("volume").alias("volume"),
        )
        .withColumn("source", lit(source_name))
    )


def build_hourly_aggregate_df(silver_df: DataFrame) -> DataFrame:
    return build_aggregate_df(silver_df, "date_trunc('hour', open_time_ts)", "spark_batch_agg_1h")


def build_daily_aggregate_df(silver_df: DataFrame) -> DataFrame:
    return build_aggregate_df(silver_df, "date_trunc('day', open_time_ts)", "spark_batch_agg_1d")


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


def build_clickhouse_aggregate_rows(aggregate_df: DataFrame) -> list[dict]:
    ready_df = aggregate_df.withColumn("bucket_start_str", date_format(col("bucket_start"), "yyyy-MM-dd HH:mm:ss.SSS"))
    return [
        {
            "symbol": row["symbol"],
            "bucket_start": row["bucket_start_str"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "source": row["source"],
        }
        for row in ready_df.select(
            "symbol",
            "bucket_start_str",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
        ).toLocalIterator()
    ]


def write_optional_snapshot(aggregate_df: DataFrame, output_root: str) -> None:
    if output_root:
        aggregate_df.write.mode("overwrite").parquet(output_root)


def run_job(args: argparse.Namespace) -> dict[str, int]:
    spark = build_spark_session(args.job_name)
    spark.sparkContext.setLogLevel("WARN")
    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    silver_df = apply_date_filter(load_silver_df(spark, args.silver_input), args.start_date, args.end_date)
    hourly_df = build_hourly_aggregate_df(silver_df)
    daily_df = build_daily_aggregate_df(silver_df)

    hourly_count = hourly_df.count()
    daily_count = daily_df.count()

    write_optional_snapshot(hourly_df, args.hourly_output)
    write_optional_snapshot(daily_df, args.daily_output)

    if not args.skip_clickhouse:
        connection = ClickHouseConnection(
            url=args.clickhouse_url,
            docker_container=args.clickhouse_docker_container,
        )
        insert_json_each_row(connection, args.clickhouse_database, args.hourly_table, build_clickhouse_aggregate_rows(hourly_df))
        insert_json_each_row(connection, args.clickhouse_database, args.daily_table, build_clickhouse_aggregate_rows(daily_df))
        metric_rows = [
            build_metric_row("hourly_aggregate_row_count", hourly_count, "rows", args.job_name, run_id),
            build_metric_row("daily_aggregate_row_count", daily_count, "rows", args.job_name, run_id),
        ]
        insert_json_each_row(connection, args.clickhouse_database, args.metrics_table, metric_rows)

    spark.stop()
    return {
        "hourly_count": hourly_count,
        "daily_count": daily_count,
    }


def main() -> None:
    result = run_job(parse_args())
    print(
        "silver_to_gold_batch completed "
        f"(hourly={result['hourly_count']}, daily={result['daily_count']})"
    )


if __name__ == "__main__":
    main()
