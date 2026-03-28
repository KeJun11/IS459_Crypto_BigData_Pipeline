"""
initial_ingestion_2.py
────────────────────────────────────────────────────────────────────────────────
Transform raw bronze data from s3://<S3_BUCKET_RAW>/bronze/ and write cleaned
Parquet to s3://<S3_BUCKET_RAW>/cleaned/bq2_daily_prices_initial_full_load/.
"""

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Configuration ─────────────────────────────────────────────────────────────
BUCKET = "is459-crypto-raw-data"
INGEST_FROM_DATE = "2025-09-21"

# ── SparkSession ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("initial-bulk-etl")
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
print(f"Default parallel  : {spark.sparkContext.defaultParallelism}")
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print("SparkSession ready")

# ── boto3 client ──────────────────────────────────────────────────────────────
s3 = boto3.client("s3")


def list_s3_folders(prefix):
    """Return immediate sub-folder prefixes under a given prefix."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, Delimiter="/")
    return [cp["Prefix"] for cp in resp.get("CommonPrefixes", [])]


# ── § 2  Ticker Discovery ─────────────────────────────────────────────────────
kaggle_tickers = {
    p.rstrip("/").split("/")[-1] for p in list_s3_folders("bronze/kaggle/btc-price-1m/")
}
binance_tickers = {
    p.rstrip("/").split("symbol=")[-1] for p in list_s3_folders("bronze/binance2/")
}

in_both = sorted(kaggle_tickers & binance_tickers)

print(f"Kaggle  : {len(kaggle_tickers):>4}  tickers")
print(f"Binance : {len(binance_tickers):>4}  tickers")
print(f"In both : {len(in_both):>4}  tickers")

# ── § 3  Kaggle Schema ────────────────────────────────────────────────────────
KAGGLE_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("close_time", TimestampType(), True),
        StructField("quote_asset_volume", DoubleType(), True),
        StructField("number_of_trades", LongType(), True),
        StructField("taker_buy_base_asset_volume", DoubleType(), True),
        StructField("taker_buy_quote_asset_volume", DoubleType(), True),
        StructField("ignore", StringType(), True),
    ]
)

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


def clean_df(df):
    """Trim symbol, drop nulls, drop duplicate (timestamp, symbol) rows."""
    return (
        df.withColumn("symbol", F.trim(F.col("symbol")))
        .dropna()
        .dropDuplicates(["timestamp", "symbol"])
    )


# ── § 4  Kaggle Preprocessing ─────────────────────────────────────────────────
KAGGLE_EXCLUDE = {"EOSUSDT", "STMXUSDT"}

kaggle_paths = [
    f"s3://{BUCKET}/{p}" for p in list_s3_folders("bronze/kaggle/btc-price-1m/")
]

kaggle_raw = (
    spark.read.schema(KAGGLE_SCHEMA)
    .csv(kaggle_paths, header=True)
    .drop("ignore")
    .withColumn(
        "symbol", F.regexp_extract(F.input_file_name(), r"/btc-price-1m/([^/]+)/", 1)
    )
    .filter(~F.col("symbol").isin(KAGGLE_EXCLUDE))
    .select(ORDERED_COLS)
)

kaggle_clean = clean_df(kaggle_raw)
print("Kaggle DataFrame ready")

# ── § 5  Binance Preprocessing ────────────────────────────────────────────────
BINANCE2_BASE = f"s3://{BUCKET}/bronze/binance2"

binance_paths = [f"s3://{BUCKET}/{p}" for p in list_s3_folders("bronze/binance2/")]

binance_raw = (
    spark.read.option("basePath", BINANCE2_BASE)
    .parquet(*binance_paths)
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

binance_clean = clean_df(binance_raw)
print("Binance DataFrame ready")

# ── § 6  Union ────────────────────────────────────────────────────────────────
combined_df = kaggle_clean.unionByName(binance_clean)

# ── § 7  Data Quality Filters ─────────────────────────────────────────────────
combined_df = combined_df.filter(
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

# ── § 8  Date Filter ──────────────────────────────────────────────────────────
# combined_df = combined_df.filter(F.to_date("timestamp") >= F.lit(INGEST_FROM_DATE))
# print(f"Applied ingestion cutoff: date >= {INGEST_FROM_DATE}")

# ── § 9  Write ────────────────────────────────────────────────────────────────
DAILY_OUT = f"s3://{BUCKET}/cleaned/bq2_daily_prices_initial_full_load"

(
    combined_df.withColumn("date", F.to_date("timestamp"))
    .write.mode("overwrite")
    .partitionBy("date")
    .parquet(DAILY_OUT)
)

print(f"Written to {DAILY_OUT}")

spark.stop()
