"""
daily_ingest.py
────────────────────────────────────────────────────────────────────────────────
Read today's records from bronze/binance2/, clean them, and append to
s3a://<BUCKET>/cleaned/bq2_daily_prices/.
"""

from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── Configuration ─────────────────────────────────────────────────────────────
BUCKET = "is459-crypto-raw-data"
TODAY = str(date.today() - timedelta(days=1))  # yesterday's complete data

# ── SparkSession ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("daily-ingest")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.connection.maximum", "200")
    .config("spark.hadoop.fs.s3a.threads.max", "200")
    .config("spark.hadoop.fs.s3a.readahead.range", "8388608")
    .config("spark.hadoop.fs.s3a.block.size", "134217728")
    .config("spark.hadoop.fs.s3a.input.fadvise", "sequential")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer.max", "512m")
    .config("spark.memory.fraction", "0.8")
    .config("spark.memory.storageFraction", "0.1")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2g")
    .config("spark.reducer.maxSizeInFlight", "100663296")
    .config("spark.shuffle.localDisk.file.output.buffer", "5242880")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
    .config("spark.sql.files.maxPartitionBytes", "134217728")
    .config("spark.sql.files.openCostInBytes", "67108864")
    .config("spark.network.timeout", "600")
    .config("spark.sql.broadcastTimeout", "600")
    .getOrCreate()
)

print(f"Spark             : {spark.version}")
print(f"Bucket (raw)      : {BUCKET}")
print(f"Ingesting date    : {TODAY}")
print("SparkSession ready")

# ── § 1  Read bronze/binance2/ ────────────────────────────────────────────────
BINANCE2_BASE = f"s3a://{BUCKET}/bronze/binance2"

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
    spark.read.option("basePath", BINANCE2_BASE)
    .parquet(BINANCE2_BASE)
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

# ── § 2  Filter to today ───────────────────────────────────────────────────────
today_df = binance_raw.filter(F.to_date("timestamp") == F.lit(TODAY))
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
DAILY_OUT = f"s3a://{BUCKET}/cleaned/bq2_daily_prices"

(
    today_df.withColumn("date", F.to_date("timestamp"))
    .write.mode("append")
    .partitionBy("date")
    .parquet(DAILY_OUT)
)

print(f"Written to {DAILY_OUT}/date={TODAY}")

spark.stop()
