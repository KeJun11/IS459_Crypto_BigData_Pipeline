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
 
# ── Shared column schema (Binance & MEXC use the same 12 fields) ────────────
 
KLINE_COLUMNS = [
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
    {"Name": "source",                          "Type": "string",  "Comment": "Exchange source e.g. binance, mexc"},
]
 
 
 
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
                "Description": "Crypto data lake – multi-exchange klines",
            }
        )
 
 
def _create_or_update_table(glue, source_name: str) -> None:
    """Create (or update) a Glue table for a given exchange source."""
    table_name = f"raw_{source_name}_klines"
    s3_location = f"s3://{settings.S3_BUCKET_RAW}/{settings.S3_RAW_PREFIX}{source_name}/"
 
    table_input = {
        "Name": table_name,
        "Description": f"Raw {source_name} daily kline data (JSON)",
        "StorageDescriptor": {
            "Columns": KLINE_COLUMNS,
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                "Parameters": {
                    "paths": ",".join(c["Name"] for c in KLINE_COLUMNS),
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
            Name=table_name,
        )
        log.info("Updating table  ➜  %s", table_name)
        glue.update_table(
            DatabaseName=settings.GLUE_DATABASE,
            TableInput=table_input,
        )
    except glue.exceptions.EntityNotFoundException:
        log.info("Creating table  ➜  %s", table_name)
        glue.create_table(
            DatabaseName=settings.GLUE_DATABASE,
            TableInput=table_input,
        )
 
 
# ── Public entry point ───────────────────────────────────────────────────────
 
def update_catalog() -> None:
    """Ensure Glue database + one table per source exist."""
    log.info("═══ Task 3: update_catalog  START ═══")
    glue = get_glue_client()
 
    _ensure_database(glue)
 
    for source_name in settings.ENABLED_SOURCES:
        _create_or_update_table(glue, source_name)
 
    log.info("═══ Task 3: update_catalog  DONE  ═══")
 
 
if __name__ == "__main__":
    update_catalog()