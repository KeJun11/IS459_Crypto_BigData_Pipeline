"""
daily_ingest.py
"""

from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── Configuration ─────────────────────────────────────────────────────────────
BUCKET = "is459-crypto-raw-data"
TODAY = str(date.today() - timedelta(days=1))

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
    .getOrCreate()
)

BINANCE2_BASE = f"s3://{BUCKET}/bronze/binance2"
DAILY_OUT = f"s3://{BUCKET}/cleaned/bq2_daily_prices_initial_full_load"
TODAY_PATH = f"{BINANCE2_BASE}/{TODAY}"

print(f"Spark             : {spark.version}")
print(f"Bucket (raw)      : {BUCKET}")
print(f"Ingesting date    : {TODAY}")
print(f"Reading from      : {TODAY_PATH}")
print("SparkSession ready")

# ── § 1  Read today's partition directly ──────────────────────────────────────
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
    spark.read.parquet(TODAY_PATH)
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

today_df = binance_raw
print(f"Filtered to date = {TODAY}")

# ── § 3  Clean ────────────────────────────────────────────────────────────────
today_df = (
    today_df.withColumn("symbol", F.trim(F.col("symbol")))
    .dropna()
    .dropDuplicates(["timestamp", "symbol"])
)

# ── § 4  Data Quality Filters ─────────────────────────────────────────────────
today_df = today_df.filter(
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
    today_df.withColumn("date", F.to_date("timestamp"))
    .write.mode("append")
    .partitionBy("date")
    .parquet(DAILY_OUT)
)

print(f"Written to {DAILY_OUT}/date={TODAY}")

spark.stop()
