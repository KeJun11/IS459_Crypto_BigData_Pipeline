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


GLUE_DATABASE = os.getenv("GLUE_DATABASE")


EMR_CLUSTER_ID = os.getenv("EMR_CLUSTER_ID")
SPARK_SCRIPT_S3_PATH = os.getenv(
    "SPARK_SCRIPT_S3_PATH",
    f"s3://{S3_BUCKET_RAW}/scripts/transform.py",
)


BINANCE_BASE_URL = "https://api.binance.com/api/v3"

BINANCE_SYMBOLS = [
    "BTCUSDT",  "ETHUSDT",  "BNBUSDT",  "XRPUSDT",  "SOLUSDT",
    "ADAUSDT",  "DOGEUSDT", "TRXUSDT",  "AVAXUSDT", "LINKUSDT",
    "DOTUSDT",  "MATICUSDT","SHIBUSDT", "LTCUSDT",  "BCHUSDT",
    "UNIUSDT",  "ATOMUSDT", "XLMUSDT",  "NEARUSDT", "ICPUSDT",
    "APTUSDT",  "FILUSDT",  "VETUSDT",  "HBARUSDT", "MANAUSDT",
    "ALGOUSDT", "QNTUSDT",  "AAVEUSDT", "EGLDUSDT", "SANDUSDT",
    "EOSUSDT",  "THETAUSDT","AXSUSDT",  "FTMUSDT",  "RUNEUSDT",
    "GALAUSDT", "FLOWUSDT", "XTZUSDT",  "CHZUSDT",  "ZILUSDT",
    "ENJUSDT",  "BATUSDT",  "COMPUSDT", "SNXUSDT",  "MKRUSDT",
    "YFIUSDT",  "SUSHIUSDT","1INCHUSDT","CRVUSDT",  "LRCUSDT",
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
]