"""
Task 3 – update_catalog
========================
Ensures the AWS Glue Data Catalog has a database and a table for
Binance kline data pointing at the S3 raw prefix.

Table is defined with JSON SerDe so Athena / Spark can query directly.
"""

from pipeline.config import settings
from pipeline.utils.aws_helpers import get_glue_client
from pipeline.utils.logger import get_logger

log = get_logger("task3.update_catalog")
 
# ── Table schema (matches Binance kline fields) ─────────────────────────────

KLINE_TABLE = {
    "name": "raw_binance_klines",
    "description": "Raw Binance 1-minute kline data (JSON)",
    "columns": [
        {"Name": "timestamp",                      "Type": "bigint",  "Comment": "Kline open time (ms UTC)"},
        {"Name": "open",                            "Type": "string",  "Comment": "Open price"},
        {"Name": "high",                            "Type": "string",  "Comment": "Highest price"},
        {"Name": "low",                             "Type": "string",  "Comment": "Lowest price"},
        {"Name": "close",                           "Type": "string",  "Comment": "Close price"},
        {"Name": "volume",                          "Type": "string",  "Comment": "Base asset volume"},
        {"Name": "close_time",                      "Type": "bigint",  "Comment": "Kline close time (ms UTC)"},
        {"Name": "quote_asset_volume",              "Type": "string",  "Comment": "Quote asset volume"},
        {"Name": "number_of_trades",                "Type": "int",     "Comment": "Number of trades"},
        {"Name": "taker_buy_base_asset_volume",     "Type": "string",  "Comment": "Taker buy base volume"},
        {"Name": "taker_buy_quote_asset_volume",    "Type": "string",  "Comment": "Taker buy quote volume"},
        {"Name": "ignore",                          "Type": "string",  "Comment": "Unused"},
        {"Name": "symbol",                          "Type": "string",  "Comment": "Trading pair e.g. BTCUSDT"},
    ],
}
 
 
 
def _ensure_database(glue) -> None:
    """Create the Glue database if it doesn't exist yet."""
    try:
        glue.get_database(Name=settings.GLUE_DATABASE)
        log.info("Database '%s' already exists", settings.GLUE_DATABASE)
    except glue.exceptions.EntityNotFoundException:
        log.info("Creating database '%s'", settings.GLUE_DATABASE)
        glue.create_database(
            DatabaseInput={
                "Name": settings.GLUE_DATABASE,
                "Description": "Crypto data lake – Binance klines",
            }
        )
 
 
def _create_or_update_table(glue) -> None:
    """Create (or update) the Binance klines table."""
    s3_location = f"s3://{settings.S3_BUCKET_RAW}/{settings.S3_RAW_PREFIX}binance/"

    table_input = {
        "Name": KLINE_TABLE["name"],
        "Description": KLINE_TABLE["description"],
        "StorageDescriptor": {
            "Columns": KLINE_TABLE["columns"],
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                "Parameters": {
                    "paths": ",".join(c["Name"] for c in KLINE_TABLE["columns"]),
                },
            },
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "json",
            "has_encrypted_data": "false",
        },
    }

    try:
        glue.get_table(
            DatabaseName=settings.GLUE_DATABASE,
            Name=KLINE_TABLE["name"],
        )
        log.info("Updating table  ➜  %s", KLINE_TABLE["name"])
        glue.update_table(
            DatabaseName=settings.GLUE_DATABASE,
            TableInput=table_input,
        )
    except glue.exceptions.EntityNotFoundException:
        log.info("Creating table  ➜  %s", KLINE_TABLE["name"])
        glue.create_table(
            DatabaseName=settings.GLUE_DATABASE,
            TableInput=table_input,
        ) 
# ── Public entry point ───────────────────────────────────────────────────────
 
def update_catalog() -> None:
    """Ensure Glue database + kline table exist and point to the right S3 path."""
    log.info("═══ Task 3: update_catalog  START ═══")
    glue = get_glue_client()

    _ensure_database(glue)
    _create_or_update_table(glue)

    log.info("═══ Task 3: update_catalog  DONE  ═══") 
if __name__ == "__main__":
    update_catalog()