"""
Task 3 – update_catalog
========================
Creates/updates two Glue tables for Parquet kline data:

1) One-time mass ingestion table from `pull_api_parquet.py`
   Layout: bronze/binance2/symbol=<SYMBOL>/date=<YYYY-MM-DD>/data.parquet

2) Daily ingestion table from `pull_parquet_daily.py`
   Layout: bronze/binance2/<YYYY-MM-DD>/data.parquet
"""

from pipeline.config import settings
from pipeline.utils.aws_helpers import get_glue_client
from pipeline.utils.logger import get_logger

log = get_logger("task3.update_catalog")

PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

BASE_PREFIX = "bronze/binance2"

COMMON_COLUMNS = [
    {"Name": "timestamp", "Type": "timestamp", "Comment": "Kline open time (UTC)"},
    {"Name": "open", "Type": "double", "Comment": "Open price"},
    {"Name": "high", "Type": "double", "Comment": "Highest price"},
    {"Name": "low", "Type": "double", "Comment": "Lowest price"},
    {"Name": "close", "Type": "double", "Comment": "Close price"},
    {"Name": "volume", "Type": "double", "Comment": "Base asset volume"},
    {"Name": "close_time", "Type": "timestamp", "Comment": "Kline close time (UTC)"},
    {"Name": "quote_asset_volume", "Type": "double", "Comment": "Quote asset volume"},
    {"Name": "number_of_trades", "Type": "bigint", "Comment": "Number of trades"},
    {"Name": "taker_buy_base_asset_volume", "Type": "double", "Comment": "Taker buy base volume"},
    {"Name": "taker_buy_quote_asset_volume", "Type": "double", "Comment": "Taker buy quote volume"},
    {"Name": "ignore", "Type": "string", "Comment": "Unused"},
]

MASS_TABLE = {
    "name": "raw_binance_klines_mass_parquet",
    "description": "Mass backfill Binance 1m klines (Parquet, partitioned by symbol/date)",
    "columns": COMMON_COLUMNS,
    "partition_keys": [
        {"Name": "symbol", "Type": "string"},
        {"Name": "date", "Type": "string"},
    ],
}

DAILY_TABLE = {
    "name": "raw_binance_klines_daily_parquet",
    "description": "Daily Binance 1m klines (Parquet, partitioned by date folder)",
    "columns": [
        *COMMON_COLUMNS,
        {"Name": "symbol", "Type": "string", "Comment": "Trading pair e.g. BTCUSDT"},
    ],
    "partition_keys": [
        {"Name": "date", "Type": "string"},
    ],
}


def _s3_uri(*parts: str) -> str:
    cleaned = [p.strip("/") for p in parts if p]
    return f"s3://{settings.S3_BUCKET_RAW}/{'/'.join(cleaned)}/"


def _ensure_database(glue) -> None:
    try:
        glue.get_database(Name=settings.GLUE_DATABASE)
        log.info("Database '%s' already exists", settings.GLUE_DATABASE)
    except glue.exceptions.EntityNotFoundException:
        log.info("Creating database '%s'", settings.GLUE_DATABASE)
        glue.create_database(
            DatabaseInput={
                "Name": settings.GLUE_DATABASE,
                "Description": "Crypto data lake – Binance klines (Parquet)",
            }
        )


def _table_input(table_def: dict) -> dict:
    return {
        "Name": table_def["name"],
        "Description": table_def["description"],
        "StorageDescriptor": {
            "Columns": table_def["columns"],
            "Location": _s3_uri(BASE_PREFIX),
            "InputFormat": PARQUET_INPUT_FORMAT,
            "OutputFormat": PARQUET_OUTPUT_FORMAT,
            "SerdeInfo": {
                "SerializationLibrary": PARQUET_SERDE,
                "Parameters": {},
            },
        },
        "PartitionKeys": table_def["partition_keys"],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "has_encrypted_data": "false",
            "EXTERNAL": "TRUE",
        },
    }


def _create_or_update_table(glue, table_def: dict) -> None:
    table_input = _table_input(table_def)
    try:
        glue.get_table(DatabaseName=settings.GLUE_DATABASE, Name=table_def["name"])
        log.info("Updating table  ➜  %s", table_def["name"])
        glue.update_table(DatabaseName=settings.GLUE_DATABASE, TableInput=table_input)
    except glue.exceptions.EntityNotFoundException:
        log.info("Creating table  ➜  %s", table_def["name"])
        glue.create_table(DatabaseName=settings.GLUE_DATABASE, TableInput=table_input)





def update_catalog() -> None:
    log.info("═══ Task 3: update_catalog  START ═══")
    glue = get_glue_client()

    _ensure_database(glue)
    _create_or_update_table(glue, MASS_TABLE)
    _create_or_update_table(glue, DAILY_TABLE)

    log.info("═══ Task 3: update_catalog  DONE  ═══")


if __name__ == "__main__":
    update_catalog()