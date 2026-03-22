"""
transform.py  –  PySpark script executed on EMR via Task 4
==========================================================
Reads raw Binance kline JSON from Glue catalog, casts numeric types,
converts epoch-ms timestamps to proper timestamps, and writes
partitioned Parquet to the cleaned S3 prefix.

Upload to:  s3://<bucket>/scripts/transform.py

Usage (via spark-submit on EMR):
    spark-submit --deploy-mode cluster --master yarn \\
        transform.py --source-db your_project_db --output-bucket your-bucket
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, IntegerType


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", required=True, help="Glue database name")
    parser.add_argument("--output-bucket", required=True, help="S3 bucket for cleaned data")
    parser.add_argument("--sources", required=True, help="Comma-separated list of sources (e.g. binance,mexc)")
    return parser.parse_args()


def clean_klines(spark, db: str, output_bucket: str, source_name: str):
    """
    Read raw_<source>_klines, cast types, convert timestamps, write Parquet.
    """
    table = f"{db}.raw_{source_name}_klines"
    print(f"Reading table: {table}")

    try:
        df = spark.table(table)
    except Exception as e:
        print(f"✗ {source_name}: could not read table – {e}")
        return

    cleaned = df.select(
        # Convert epoch-ms → UTC timestamp
        (F.col("timestamp") / 1000).cast("timestamp").alias("timestamp"),
        (F.col("close_time") / 1000).cast("timestamp").alias("close_time"),

        # Cast price / volume strings → doubles
        F.col("open").cast(DoubleType()).alias("open"),
        F.col("high").cast(DoubleType()).alias("high"),
        F.col("low").cast(DoubleType()).alias("low"),
        F.col("close").cast(DoubleType()).alias("close"),
        F.col("volume").cast(DoubleType()).alias("volume"),
        F.col("quote_asset_volume").cast(DoubleType()).alias("quote_asset_volume"),
        F.col("number_of_trades").cast(IntegerType()).alias("number_of_trades"),
        F.col("taker_buy_base_asset_volume").cast(DoubleType()).alias("taker_buy_base_asset_volume"),
        F.col("taker_buy_quote_asset_volume").cast(DoubleType()).alias("taker_buy_quote_asset_volume"),

        # Keep symbol and source for partitioning
        F.col("symbol"),
        F.col("source"),

        # Add ingestion metadata
        F.current_date().alias("ingestion_date"),
    )

    output_path = f"s3://{output_bucket}/cleaned/{source_name}_klines/"
    print(f"Writing to: {output_path}")

    cleaned.write \
        .mode("overwrite") \
        .partitionBy("symbol", "ingestion_date") \
        .parquet(output_path)

    row_count = cleaned.count()
    print(f"✓ {source_name}: {row_count} rows written")


def main():
    args = parse_args()
    sources = [s.strip() for s in args.sources.split(",")]

    spark = (
        SparkSession.builder
        .appName("multi-source-klines-transform")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    print(f"Source DB     : {args.source_db}")
    print(f"Output bucket : {args.output_bucket}")
    print(f"Sources       : {sources}")

    for source_name in sources:
        print(f"\n{'='*40}")
        print(f"Processing: {source_name}")
        print(f"{'='*40}")
        clean_klines(spark, args.source_db, args.output_bucket, source_name)

    spark.stop()
    print("\nAll transforms complete.")


if __name__ == "__main__":
    main()