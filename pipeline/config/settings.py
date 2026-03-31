"""
Pipeline Configuration
======================
Replace placeholders with real values once AWS is provisioned.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (one level up from config/)
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW")
S3_RAW_PREFIX = "raw/"

S3_CLEANED_BUCKET = os.getenv("S3_CLEANED_BUCKET", 'is459-crypto-raw-data') 

CLICKHOUSE_PROTOCOL = os.getenv("CLICKHOUSE_PROTOCOL", "http")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1") #change to clickhouse EC2 public ip
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv(
    "CLICKHOUSE_DB",
    os.getenv("CLICKHOUSE_DATABASE", "cleaned_crypto"),
)
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_RAW_OHLCV_TABLE = os.getenv("CLICKHOUSE_RAW_OHLCV_TABLE", "ohlcv_1min")
CLICKHOUSE_LOAD_MODE = os.getenv("CLICKHOUSE_LOAD_MODE", "append").lower()
CLICKHOUSE_HTTP_TIMEOUT = int(os.getenv("CLICKHOUSE_HTTP_TIMEOUT", "7200"))
CLICKHOUSE_SOURCE_URL = os.getenv(
    "CLICKHOUSE_SOURCE_URL",
    (
        f"https://{S3_CLEANED_BUCKET}.s3.{AWS_REGION}.amazonaws.com/"
        "cleaned/bq2_daily_prices_initial_full_load/date=*/*.parquet"
    )
    if S3_CLEANED_BUCKET
    else "",
)


GLUE_DATABASE = os.getenv("GLUE_DATABASE")


EMR_CLUSTER_ID = os.getenv("EMR_CLUSTER_ID")
SPARK_SCRIPT_S3_PATH = os.getenv(
    "SPARK_SCRIPT_S3_PATH",
    f"s3://{S3_BUCKET_RAW}/scripts/transform.py",
)


BINANCE_BASE_URL = "https://api.binance.com/api/v3"

BINANCE_SYMBOLS = [
    "AAVEUSDT",  "ADAUSDT",   "ALGOUSDT",  "ATOMUSDT",  "AVAXUSDT",
    "BATUSDT",   "BCHUSDT",   "BNBUSDT",   "BTCUSDT",   "CAKEUSDT",
    "CHZUSDT",   "CRVUSDT",   "DASHUSDT",  "DEXEUSDT",  "DOGEUSDT",
    "DOTUSDT",   "ENAUSDT",   "ENJUSDT",   "EOSUSDT",   "ETCUSDT",
    "ETHUSDT",   "FILUSDT",   "GRTUSDT",   "HBARUSDT",  "ICPUSDT",
    "IOSTUSDT",  "IOTAUSDT",  "LINKUSDT",  "LTCUSDT",   "MANAUSDT",
    "NEARUSDT",  "NEOUSDT",   "QTUMUSDT",  "RVNUSDT",   "SANDUSDT",
    "SHIBUSDT",  "SOLUSDT",   "STMXUSDT",  "SUSHIUSDT", "TFUELUSDT",
    "THETAUSDT", "TIAUSDT",   "TRXUSDT",   "UNIUSDT",   "VETUSDT",
    "XLMUSDT",   "XRPUSDT",   "XTZUSDT",   "ZECUSDT",   "ZILUSDT",
]


# Kline interval: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d, etc.
BINANCE_INTERVAL = "1m"

# Max klines per request (Binance caps at 1000)
BINANCE_LIMIT = 1000

# Start date for kline pull (2025-09-21)
BACKFILL_START_DATE = "2025-09-21"

# Column names matching Binance kline response (index 0–11)
KLINE_COLUMNS = [
    "timestamp",                       # 0  – Kline open time (ms)
    "open",                            # 1
    "high",                            # 2
    "low",                             # 3
    "close",                           # 4
    "volume",                          # 5  – Base asset volume
    "close_time",                      # 6  – Kline close time (ms)
    "quote_asset_volume",              # 7
    "number_of_trades",                # 8
    "taker_buy_base_asset_volume",     # 9
    "taker_buy_quote_asset_volume",    # 10
    "ignore",                          # 11 – Unused
]



