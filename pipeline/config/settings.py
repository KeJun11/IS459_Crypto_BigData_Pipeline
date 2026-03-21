"""
Pipeline Configuration
======================
Replace placeholders with real values once AWS is provisioned.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
# ──────────────────────────────────────────────
# AWS Credentials (placeholder – swap when ready)
# ──────────────────────────────────────────────
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "YOUR_AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "YOUR_AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# ──────────────────────────────────────────────
# S3 (placeholder – swap when bucket is created)
# ──────────────────────────────────────────────
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "your-project-raw-data-bucket")
S3_RAW_PREFIX = "raw/"

# ──────────────────────────────────────────────
# Glue Catalog
# ──────────────────────────────────────────────
GLUE_DATABASE = os.getenv("GLUE_DATABASE", "your_project_db")

# ──────────────────────────────────────────────
# EMR / Spark
# ──────────────────────────────────────────────
EMR_CLUSTER_ID = os.getenv("EMR_CLUSTER_ID", "j-XXXXXXXXXXXXX")
SPARK_SCRIPT_S3_PATH = os.getenv(
    "SPARK_SCRIPT_S3_PATH",
    f"s3://{S3_BUCKET_RAW}/scripts/transform.py",
)

# ──────────────────────────────────────────────
# Binance API (free, no key required)
# ──────────────────────────────────────────────
BINANCE_BASE_URL = "https://api.binance.com/api/v3"

# Trading pairs to pull (against USDT)
BINANCE_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "DOTUSDT",
    "LINKUSDT",
]

# Kline interval: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d, etc.
BINANCE_INTERVAL = "1m"

# Max klines per request (Binance caps at 1000)
BINANCE_LIMIT = 1000

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
    "ignore",                          # 11
]