"""
Task 5 - load_to_clickhouse
===========================
Create the serving table in ClickHouse and load cleaned Parquet directly
from S3 into ClickHouse using the built-in s3() table function.
"""

import re

import requests

from pipeline.config import settings
from pipeline.utils.logger import get_logger

log = get_logger("task5.load_to_clickhouse")

VALID_LOAD_MODES = {"replace", "append"}
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
        date Date MATERIALIZED toDate(timestamp),
        ingested_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(ingested_at)
    PARTITION BY toYYYYMM(date)
    ORDER BY (symbol, timestamp)
    """


def _truncate_table_query(table_ref: str) -> str:
    return f"TRUNCATE TABLE {table_ref}"


def _watermark_query(table_ref: str) -> str:
    return f"""
    SELECT
        if(count() = 0, '', toString(max(date))) AS max_date,
        if(
            count() = 0,
            '',
            formatDateTime(max(timestamp), '%Y-%m-%d %H:%i:%S.%f')
        ) AS max_timestamp
    FROM {table_ref}
    """


def _row_count_query(table_ref: str) -> str:
    return f"SELECT count() FROM {table_ref}"


def _current_watermark(table_ref: str) -> tuple[str | None, str | None]:
    result = _execute(_watermark_query(table_ref), label="read current watermark")
    if not result:
        return None, None

    parts = result.split("\t")
    max_date = parts[0].strip() if parts else ""
    max_timestamp = parts[1].strip() if len(parts) > 1 else ""
    return max_date or None, max_timestamp or None


def _incremental_where_clause(
    max_date: str | None,
    max_timestamp: str | None,
) -> str:
    if not max_date or not max_timestamp:
        return ""

    return f"""
    WHERE
        date > toDate({_sql_string(max_date)})
        OR (
            date = toDate({_sql_string(max_date)})
            AND CAST(timestamp AS DateTime64(3)) > toDateTime64({_sql_string(max_timestamp)}, 3)
        )
    """


def _insert_query(table_ref: str, source_url: str, where_clause: str = "") -> str:
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
    {where_clause}
    """


def load_to_clickhouse() -> int:
    database = _validate_identifier("CLICKHOUSE_DB", settings.CLICKHOUSE_DB)
    table = _validate_identifier(
        "CLICKHOUSE_RAW_OHLCV_TABLE",
        settings.CLICKHOUSE_RAW_OHLCV_TABLE,
    )
    source_url = settings.CLICKHOUSE_SOURCE_URL
    load_mode = settings.CLICKHOUSE_LOAD_MODE
    table_ref = f"{database}.{table}"

    if not source_url:
        raise ValueError(
            "CLICKHOUSE_SOURCE_URL is empty. Set S3_CLEANED_BUCKET or CLICKHOUSE_SOURCE_URL."
        )
    if load_mode not in VALID_LOAD_MODES:
        raise ValueError(
            f"CLICKHOUSE_LOAD_MODE must be one of {sorted(VALID_LOAD_MODES)}"
        )

    log.info("=== Task 5: load_to_clickhouse START ===")
    log.info("Target ClickHouse: %s", _clickhouse_base_url())
    log.info("Target table: %s", table_ref)
    log.info("Source parquet: %s", source_url)
    log.info("Load mode: %s", load_mode)

    _ping_clickhouse()
    log.info("ClickHouse ping successful")

    _execute(_create_database_query(database), label="create database")
    _execute(_create_table_query(table_ref), label="create table")

    if load_mode == "replace":
        log.info("Replacing existing rows in %s", table_ref)
        _execute(_truncate_table_query(table_ref), label="truncate table")
        where_clause = ""
        rows_before = 0
    else:
        rows_before = int(_execute(_row_count_query(table_ref), label="count rows before load"))
        max_date, max_timestamp = _current_watermark(table_ref)
        if max_date and max_timestamp:
            log.info(
                "Append mode watermark: max_date=%s, max_timestamp=%s",
                max_date,
                max_timestamp,
            )
        else:
            log.info("Append mode found an empty target table; loading full dataset")
        where_clause = _incremental_where_clause(max_date, max_timestamp)

    _execute(
        _insert_query(table_ref, source_url, where_clause),
        label="load parquet from s3",
    )

    row_count = int(_execute(_row_count_query(table_ref), label="count rows after load"))
    if load_mode == "append":
        new_rows = row_count - rows_before
        log.info("Append mode inserted %d new rows", new_rows)
    log.info("=== Task 5: load_to_clickhouse DONE === (%d rows in %s)", row_count, table_ref)
    return row_count


if __name__ == "__main__":
    total_rows = load_to_clickhouse()
    print(f"Rows in ClickHouse table: {total_rows}")
