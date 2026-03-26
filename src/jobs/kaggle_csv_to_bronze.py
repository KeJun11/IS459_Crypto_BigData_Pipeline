from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    expr,
    lit,
    month,
    regexp_replace,
    to_timestamp,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


CSV_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), nullable=True),
        StructField("open", DoubleType(), nullable=True),
        StructField("high", DoubleType(), nullable=True),
        StructField("low", DoubleType(), nullable=True),
        StructField("close", DoubleType(), nullable=True),
        StructField("volume", DoubleType(), nullable=True),
        StructField("close_time", StringType(), nullable=True),
        StructField("quote_asset_volume", DoubleType(), nullable=True),
        StructField("number_of_trades", DoubleType(), nullable=True),
        StructField("taker_buy_base_asset_volume", DoubleType(), nullable=True),
        StructField("taker_buy_quote_asset_volume", DoubleType(), nullable=True),
        StructField("ignore", StringType(), nullable=True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Kaggle OHLCV CSV seed data into Bronze parquet for the batch shakedown."
    )
    parser.add_argument("--input-path", required=True, help="Local path or S3 URI to the Kaggle CSV input.")
    parser.add_argument("--output-root", required=True, help="Local path or S3 URI for the Bronze parquet output.")
    parser.add_argument("--symbol", default="", help="Optional symbol override, for example BTCUSDT.")
    parser.add_argument("--source", default="kaggle_csv")
    parser.add_argument("--job-name", default="kaggle_csv_to_bronze")
    parser.add_argument("--write-mode", choices=["overwrite", "append"], default="overwrite")
    return parser.parse_args()


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )


def resolve_symbol(explicit_symbol: str, input_path: str) -> str:
    if explicit_symbol:
        return explicit_symbol.strip().upper()

    candidates = []
    for part in re.split(r"[\\/._=\-]+", input_path.upper()):
        if re.fullmatch(r"[A-Z0-9]{5,20}", part) and any(suffix in part for suffix in ("USDT", "USD", "BTC", "ETH")):
            candidates.append(part)

    if not candidates:
        raise ValueError(
            "Unable to infer symbol from --input-path. Pass --symbol explicitly for single-symbol Kaggle datasets."
        )
    return candidates[0]


def load_csv_df(spark: SparkSession, input_path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(CSV_SCHEMA)
        .csv(input_path)
    )


def parse_datetime_column(column_name: str) -> Column:
    # Kaggle exports may use microsecond strings such as 2025-04-06 09:40:59.999000.
    # Trim them to millisecond precision so Spark 3 parses them consistently on EMR.
    normalized_value = regexp_replace(col(column_name), r"(\.\d{3})\d+$", "$1")
    return coalesce(
        to_timestamp(normalized_value, "yyyy-MM-dd HH:mm:ss.SSS"),
        to_timestamp(normalized_value, "yyyy-MM-dd HH:mm:ss"),
    )


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
        & (col("low") <= col("high"))
        & (col("open_time") <= col("close_time"))
        & (col("volume") >= lit(0.0))
    )


def build_bronze_df(csv_df: DataFrame, symbol: str, source: str, ingested_at_ms: int) -> DataFrame:
    normalized_df = (
        csv_df.withColumn("open_time_ts", parse_datetime_column("timestamp"))
        .withColumn("close_time_ts", parse_datetime_column("close_time"))
        .withColumn("symbol", lit(symbol))
        .withColumn("open_time", expr("unix_millis(open_time_ts)"))
        .withColumn("close_time", expr("unix_millis(close_time_ts)"))
        .withColumn("is_closed", lit(True))
        .withColumn("source", lit(source))
        .withColumn("ingested_at", lit(int(ingested_at_ms)))
        .select(
            "symbol",
            col("open").cast("double").alias("open"),
            col("high").cast("double").alias("high"),
            col("low").cast("double").alias("low"),
            col("close").cast("double").alias("close"),
            col("volume").cast("double").alias("volume"),
            col("open_time").cast("long").alias("open_time"),
            col("close_time").cast("long").alias("close_time"),
            "is_closed",
            "source",
            col("ingested_at").cast("long").alias("ingested_at"),
            "open_time_ts",
        )
    )
    return normalized_df.filter(build_record_validity_condition())


def write_bronze_parquet(bronze_df: DataFrame, output_root: str, write_mode: str) -> None:
    partitioned_df = (
        bronze_df.withColumn("year", year(col("open_time_ts")))
        .withColumn("month", month(col("open_time_ts")))
        .drop("open_time_ts")
    )
    (
        partitioned_df.write.mode(write_mode)
        .partitionBy("symbol", "year", "month")
        .parquet(output_root)
    )


def run_job(args: argparse.Namespace) -> dict[str, int | str]:
    spark = build_spark_session(args.job_name)
    spark.sparkContext.setLogLevel("WARN")
    try:
        symbol = resolve_symbol(args.symbol, args.input_path)
        ingested_at_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        csv_df = load_csv_df(spark, args.input_path)
        input_row_count = csv_df.count()
        bronze_df = build_bronze_df(csv_df, symbol, args.source, ingested_at_ms)
        output_row_count = bronze_df.count()

        if output_row_count == 0:
            raise ValueError("Kaggle conversion produced zero valid Bronze rows.")

        write_bronze_parquet(bronze_df, args.output_root, args.write_mode)
        return {
            "symbol": symbol,
            "input_row_count": input_row_count,
            "output_row_count": output_row_count,
        }
    finally:
        spark.stop()


def main() -> None:
    result = run_job(parse_args())
    print(
        "kaggle_csv_to_bronze completed "
        f"(symbol={result['symbol']}, input={result['input_row_count']}, output={result['output_row_count']})"
    )


if __name__ == "__main__":
    main()
