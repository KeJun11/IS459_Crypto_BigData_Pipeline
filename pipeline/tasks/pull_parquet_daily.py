"""
pull_api_parquet (daily incremental)
==============================================
Fetches the previous day's 1-minute kline data from Binance for ALL
symbols in settings.BINANCE_SYMBOLS and uploads everything into a
single Parquet file per day.

Triggered daily by Airflow at 00:00 UTC. Each run pulls the previous
complete UTC day (midnight to midnight).

S3 layout:
    s3://is459-crypto-raw-data/bronze/binance2/<YYYY-MM-DD>/data.parquet

Each file contains all symbols, distinguished by the 'symbol' column.
"""

import time
import datetime as dt
from io import BytesIO
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import requests

from pipeline.config import settings
from pipeline.utils.logger import get_logger

log = get_logger("task.pull_api_parquet")


# ── Type mapping (single source of truth) ──────────────────────────────────
_TIMESTAMP_COLUMNS = {"timestamp", "close_time"}
_INT_COLUMNS = {"number_of_trades"}
_FLOAT_COLUMNS = {
    "open", "high", "low", "close", "volume",
    "quote_asset_volume",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
}


def _build_kline_schema() -> pa.Schema:
    fields = []
    for col in settings.KLINE_COLUMNS:
        if col in _TIMESTAMP_COLUMNS:
            fields.append((col, pa.timestamp("ms", tz="UTC")))
        elif col in _INT_COLUMNS:
            fields.append((col, pa.int64()))
        elif col in _FLOAT_COLUMNS:
            fields.append((col, pa.float64()))
        else:
            fields.append((col, pa.string()))
    fields.append(("symbol", pa.string()))
    return pa.schema(fields)


KLINE_SCHEMA = _build_kline_schema()


# ── S3 helper ──────────────────────────────────────────────────────────────
_s3_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _s3_key_exists(bucket: str, key: str) -> bool:
    """Check if an object already exists in S3."""
    try:
        _get_s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except _get_s3_client().exceptions.ClientError:
        return False


def _upload_parquet_to_s3(
    records: list[dict[str, Any]],
    date_str: str,
) -> str:
    typed_records = _cast_record_types(records)
    table = pa.Table.from_pylist(typed_records, schema=KLINE_SCHEMA)

    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    bucket = getattr(settings, "S3_BUCKET_RAW", "is459-crypto-raw-data")
    key = f"bronze/binance2/{date_str}/data.parquet"

    _get_s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
    )
    return f"s3://{bucket}/{key}"


def _cast_record_types(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    typed: list[dict[str, Any]] = []
    for rec in records:
        row = {}
        for k, v in rec.items():
            if k in _TIMESTAMP_COLUMNS:
                row[k] = int(v)
            elif k in _INT_COLUMNS:
                row[k] = int(v)
            elif k in _FLOAT_COLUMNS:
                row[k] = float(v)
            else:
                row[k] = str(v) if v is not None else None
        typed.append(row)
    return typed


# ── Binance helpers ────────────────────────────────────────────────────────

def _pull_klines_page(symbol: str, start_time: int, end_time: int) -> list[list]:
    url = f"{settings.BINANCE_BASE_URL}/klines"
    params = {
        "symbol": symbol,
        "interval": settings.BINANCE_INTERVAL,
        "limit": settings.BINANCE_LIMIT,
        "startTime": start_time,
        "endTime": end_time,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _pull_klines_daily(symbol: str, target_date: dt.date) -> list[dict[str, Any]]:
    """
    Fetch all 1m klines for a single UTC day (midnight to midnight).
    A full day = 1440 candles = 2 API pages at limit=1000.
    """
    start_ms = int(
        dt.datetime.combine(target_date, dt.time.min, tzinfo=dt.timezone.utc)
        .timestamp() * 1000
    )
    end_ms = int(
        dt.datetime.combine(target_date + dt.timedelta(days=1), dt.time.min, tzinfo=dt.timezone.utc)
        .timestamp() * 1000
    ) - 1

    all_records: list[dict[str, Any]] = []
    cursor = start_ms
    page = 0

    while cursor <= end_ms:
        page += 1
        try:
            raw_klines = _pull_klines_page(symbol, cursor, end_ms)
        except Exception as e:
            log.error("  %s  page %d  ✗  %s", symbol, page, e)
            break

        if not raw_klines:
            break

        for kline in raw_klines:
            record = {
                col: kline[i] for i, col in enumerate(settings.KLINE_COLUMNS)
            }
            record["symbol"] = symbol
            all_records.append(record)

        last_open_time = raw_klines[-1][0]
        cursor = last_open_time + 1

        log.info("  %s  page %d  ✓  %d klines", symbol, page, len(raw_klines))

        if len(raw_klines) < settings.BINANCE_LIMIT:
            break

        time.sleep(0.1)

    return all_records


# ── Main entry point ───────────────────────────────────────────────────────

def pull_api_data(target_date: dt.date | None = None) -> str:
    """
    Pull previous day's klines for ALL symbols and upload as a single
    Parquet file to S3.

    Args:
        target_date: The UTC date to pull. Defaults to yesterday.

    Returns:
        The S3 path of the uploaded file.
    """
    if target_date is None:
        target_date = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)

    date_str = target_date.isoformat()

    # Skip if file already exists in S3
    bucket = getattr(settings, "S3_BUCKET_RAW", "is459-crypto-raw-data")
    key = f"bronze/binance2/{date_str}/data.parquet"
    if _s3_key_exists(bucket, key):
        s3_path = f"s3://{bucket}/{key}"
        log.info("═══ Task 1: SKIPPED (%s) ═══  file already exists: %s", date_str, s3_path)
        return s3_path

    log.info("═══ Task 1: pull_api_data  START  (%s) ═══", date_str)

    all_records: list[dict[str, Any]] = []

    for i, symbol in enumerate(settings.BINANCE_SYMBOLS, 1):
        log.info("[%d/%d]  %s", i, len(settings.BINANCE_SYMBOLS), symbol)
        try:
            records = _pull_klines_daily(symbol, target_date)
            all_records.extend(records)
            log.info("  ✓  %s  →  %d klines", symbol, len(records))
        except Exception as e:
            log.error("  ✗  %s  →  %s", symbol, e)

    if not all_records:
        log.warning("No data collected for %s — skipping upload", date_str)
        return ""

    s3_path = _upload_parquet_to_s3(all_records, date_str)
    log.info("═══ Task 1: pull_api_data  DONE  ═══  (%d klines → %s)",
             len(all_records), s3_path)
    return s3_path


if __name__ == "__main__":
    import sys

    # Optional: pass a date to backfill a specific day
    # Usage: python3 -m pipeline.tasks.pull_api_parquet 2026-03-28
    if len(sys.argv) > 1:
        target = dt.date.fromisoformat(sys.argv[1])
    else:
        target = None

    result = pull_api_data(target)
    print(f"\n  → {result or 'NOTHING UPLOADED'}")