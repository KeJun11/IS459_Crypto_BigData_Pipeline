"""
Task 5 - load_to_clickhouse
===========================
Create the serving table in ClickHouse and load cleaned Parquet for a single
date partition from S3. ReplacingMergeTree handles deduplication on re-runs.
"""

import re
from datetime import datetime, timedelta, timezone

import requests

from pipeline.config import settings
from pipeline.utils.logger import get_logger

log = get_logger("task5.load_to_clickhouse")

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, value: str) -> str:
    if not value or not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{name} must contain only letters, numbers, and underscores")
    return value


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _clickhouse_base_url() -> str:
    return (
        f"{settings.CLICKHOUSE_PROTOCOL}://"
        f"{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}"
    )


def _auth():
    return (settings.CLICKHOUSE_USER, settings.CLICKHOUSE_PASSWORD)


def _session() -> requests.Session:
    session = requests.Session()
    # This machine has proxy env vars pointing at a dead local proxy.
    session.trust_env = False
    return session


def _execute(query: str, *, label: str) -> str:
    with _session() as session:
        response = session.post(
            f"{_clickhouse_base_url()}/",
            auth=_auth(),
            data=query.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=settings.CLICKHOUSE_HTTP_TIMEOUT,
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        detail = response.text.strip() or response.reason
        raise RuntimeError(f"ClickHouse query failed during {label}: {detail}") from exc
    return response.text.strip()


def _ping_clickhouse() -> None:
    with _session() as session:
        response = session.get(
            f"{_clickhouse_base_url()}/ping",
            auth=_auth(),
            timeout=10,
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError("ClickHouse ping failed") from exc

    if response.text.strip() != "Ok.":
        raise RuntimeError(f"Unexpected ClickHouse ping response: {response.text.strip()}")


def _create_database_query(database: str) -> str:
    return f"CREATE DATABASE IF NOT EXISTS {database}"


def _create_table_query(table_ref: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref}
    (
        timestamp DateTime64(3),
        symbol LowCardinality(String),
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        close_time DateTime64(3),
        quote_asset_volume Float64,
        number_of_trades UInt64,
        taker_buy_base_asset_volume Float64,
        taker_buy_quote_asset_volume Float64,
        date Date MATERIALIZED toDate(timestamp, 'UTC'),
        ingested_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingested_at)
    PARTITION BY toYYYYMM(date)
    ORDER BY (symbol, timestamp)
    """


def _row_count_query(table_ref: str) -> str:
    return f"SELECT count() FROM {table_ref}"


def _insert_query(table_ref: str, source_url: str) -> str:
    return f"""
    INSERT INTO {table_ref}
    (
        timestamp,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        close_time,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume
    )
    SELECT
        CAST(timestamp AS DateTime64(3)) AS timestamp,
        trim(symbol) AS symbol,
        CAST(open AS Float64) AS open,
        CAST(high AS Float64) AS high,
        CAST(low AS Float64) AS low,
        CAST(close AS Float64) AS close,
        CAST(volume AS Float64) AS volume,
        CAST(close_time AS DateTime64(3)) AS close_time,
        CAST(quote_asset_volume AS Float64) AS quote_asset_volume,
        CAST(number_of_trades AS UInt64) AS number_of_trades,
        CAST(taker_buy_base_asset_volume AS Float64) AS taker_buy_base_asset_volume,
        CAST(taker_buy_quote_asset_volume AS Float64) AS taker_buy_quote_asset_volume
    FROM s3({_sql_string(source_url)}, 'Parquet')
    """


def load_to_clickhouse(target_date: str | None = None, **kwargs) -> int:
    database = _validate_identifier("CLICKHOUSE_DB", settings.CLICKHOUSE_DB)
    table = _validate_identifier(
        "CLICKHOUSE_RAW_OHLCV_TABLE",
        settings.CLICKHOUSE_RAW_OHLCV_TABLE,
    )
    resolved_date = target_date or str(datetime.now(timezone.utc).date() - timedelta(days=1))

    if not settings.S3_CLEANED_BASE_URL:
        raise ValueError("S3_CLEANED_BUCKET is not set")
    source_url = f"{settings.S3_CLEANED_BASE_URL}/date={resolved_date}/*.parquet"

    table_ref = f"{database}.{table}"

    log.info("=== Task 5: load_to_clickhouse START ===")
    log.info("Target ClickHouse: %s", _clickhouse_base_url())
    log.info("Target table: %s", table_ref)
    log.info("Source parquet: %s", source_url)

    _ping_clickhouse()
    log.info("ClickHouse ping successful")

    _execute(_create_database_query(database), label="create database")
    _execute(_create_table_query(table_ref), label="create table")

    rows_before = int(_execute(_row_count_query(table_ref), label="count rows before load"))
    _execute(_insert_query(table_ref, source_url), label="load parquet from s3")
    row_count = int(_execute(_row_count_query(table_ref), label="count rows after load"))

    log.info("Inserted %d new rows", row_count - rows_before)
    log.info("=== Task 5: load_to_clickhouse DONE === (%d rows in %s)", row_count, table_ref)
    return row_count


if __name__ == "__main__":
    total_rows = load_to_clickhouse()
    print(f"Rows in ClickHouse table: {total_rows}")
