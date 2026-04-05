"""
daily_ingest.py
"""

import sys
from datetime import datetime, timezone, timedelta

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── Configuration ─────────────────────────────────────────────────────────────
BUCKET = "is459-crypto-raw-data"
YESTERDAY = sys.argv[1] if len(sys.argv) > 1 else str(datetime.now(timezone.utc).date() - timedelta(days=1))

# ── Guard: skip if output partition already exists ────────────────────────────
_out_prefix = f"cleaned/bq2_daily_prices_initial_full_load/date={YESTERDAY}/"
_s3 = boto3.client("s3")
_existing = _s3.list_objects_v2(Bucket=BUCKET, Prefix=_out_prefix, MaxKeys=1)
if _existing.get("KeyCount", 0) > 0:
    print(
        f"SKIPPED: output partition s3://{BUCKET}/{_out_prefix} already exists. Exiting to prevent duplicates."
    )
    sys.exit(0)

# ── SparkSession ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("daily-ingest")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer.max", "512m")
    .config("spark.memory.fraction", "0.8")
    .config("spark.memory.storageFraction", "0.1")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2147483648")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
    .config("spark.sql.files.maxPartitionBytes", "134217728")
    .config("spark.sql.broadcastTimeout", "600")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

BINANCE2_BASE = f"s3://{BUCKET}/bronze/binance2"
DAILY_OUT = f"s3://{BUCKET}/cleaned/bq2_daily_prices_initial_full_load"
YESTERDAY_PATH = f"{BINANCE2_BASE}/{YESTERDAY}"

print(f"Spark             : {spark.version}")
print(f"Bucket (raw)      : {BUCKET}")
print(f"Ingesting date    : {YESTERDAY}")
print(f"Reading from      : {YESTERDAY_PATH}")
print("SparkSession ready")

# ── § 1  Read yesterday's partition ───────────────────────────────────────────
ORDERED_COLS = [
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]

binance_raw = (
    spark.read.parquet(YESTERDAY_PATH)
    .select(
        F.col("timestamp"),
        F.col("symbol"),
        F.col("open").cast(DoubleType()),
        F.col("high").cast(DoubleType()),
        F.col("low").cast(DoubleType()),
        F.col("close").cast(DoubleType()),
        F.col("volume").cast(DoubleType()),
        F.col("close_time"),
        F.col("quote_asset_volume").cast(DoubleType()),
        F.col("number_of_trades"),
        F.col("taker_buy_base_asset_volume").cast(DoubleType()),
        F.col("taker_buy_quote_asset_volume").cast(DoubleType()),
    )
    .select(ORDERED_COLS)
)

yesterday_df = binance_raw
print(f"Ingesting date = {YESTERDAY}")

# ── § 3  Clean ────────────────────────────────────────────────────────────────
yesterday_df = (
    yesterday_df.withColumn("symbol", F.trim(F.col("symbol")))
    .dropna()
    .dropDuplicates(["timestamp", "symbol"])
)

# ── § 4  Data Quality Filters ─────────────────────────────────────────────────
yesterday_df = yesterday_df.filter(
    (F.col("open") > 0)
    & (F.col("high") > 0)
    & (F.col("low") > 0)
    & (F.col("close") > 0)
    & (F.col("high") >= F.col("low"))
    & (F.col("high") >= F.col("open"))
    & (F.col("high") >= F.col("close"))
    & (F.col("low") <= F.col("open"))
    & (F.col("low") <= F.col("close"))
    & (F.col("volume") >= 0)
)

# ── § 5  Write ────────────────────────────────────────────────────────────────
(
    yesterday_df.withColumn("date", F.to_date("timestamp"))
    .write.mode("append")
    .partitionBy("date")
    .parquet(DAILY_OUT)
)

print(f"Written to {DAILY_OUT}/date={YESTERDAY}")

spark.stop()
