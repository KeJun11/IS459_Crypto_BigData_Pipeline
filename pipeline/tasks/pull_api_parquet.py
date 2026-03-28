"""
Task 1 – pull_api_data
======================
Pulls 1-minute kline (candlestick) data from the Binance public API
for every symbol in settings.BINANCE_SYMBOLS, backfilling from
BACKFILL_START_DATE to the current time using pagination.

Binance /api/v3/klines returns arrays of 12 elements per kline:
    [open_time, open, high, low, close, volume, close_time,
     quote_asset_volume, number_of_trades,
     taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore]

This task converts each array to a dict using settings.KLINE_COLUMNS,
then uploads the result as partitioned Parquet files to S3.
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

log = get_logger("task1.pull_api_parquet")

# ── Arrow schema for kline data ────────────────────────────────────────────
# Built from settings.KLINE_COLUMNS so there's a single source of truth.
# Type map: which columns are timestamps, ints, floats — rest default to string.
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
    return pa.schema(fields)


KLINE_SCHEMA = _build_kline_schema()


# ── S3 helper ──────────────────────────────────────────────────────────────
_s3_client = None


def _get_s3_client():
    """Lazy-init a boto3 S3 client (reused across uploads)."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _upload_parquet_to_s3(
    records: list[dict[str, Any]],
    symbol: str,
) -> str:
    """
    Convert records to a Parquet buffer and upload to S3.

    S3 key layout:
        s3://<bucket>/klines/symbol=<SYMBOL>/date=<YYYY-MM-DD>/data.parquet

    Uses the current UTC date as the partition date.
    Returns the full S3 key that was written.
    """
    # Cast numeric strings coming from Binance to proper Python types
    typed_records = _cast_record_types(records)

    table = pa.Table.from_pylist(typed_records, schema=KLINE_SCHEMA)

    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    date_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    prefix = getattr(settings, "S3_PREFIX", "bronze/binance2")
    key = f"{prefix}/symbol={symbol}/date={date_str}/data.parquet"

    bucket = getattr(settings, "S3_BUCKET_RAW", "is459-crypto-raw-data")

    _get_s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
    )
    return f"s3://{bucket}/{key}"


def _cast_record_types(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Binance returns all kline fields as strings.
    Cast them to native Python types so Arrow doesn't have to guess.
    Uses _TIMESTAMP_COLUMNS, _INT_COLUMNS, and _FLOAT_COLUMNS (single source of truth).
    """
    typed: list[dict[str, Any]] = []
    for rec in records:
        row = {}
        for k, v in rec.items():
            if k in _TIMESTAMP_COLUMNS:
                row[k] = int(v)  # epoch ms — Arrow converts to timestamp
            elif k in _INT_COLUMNS:
                row[k] = int(v)
            elif k in _FLOAT_COLUMNS:
                row[k] = float(v)
            else:
                row[k] = str(v) if v is not None else None
        typed.append(row)
    return typed


# ── Binance helpers (unchanged) ────────────────────────────────────────────

def _parse_start_time() -> int:
    date_obj = dt.datetime.strptime(settings.BACKFILL_START_DATE, "%Y-%m-%d")
    date_obj = date_obj.replace(tzinfo=dt.timezone.utc)
    return int(date_obj.timestamp() * 1000)


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


def _pull_klines_full(symbol: str) -> list[dict[str, Any]]:
    end_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    start_ms = _parse_start_time()

    all_records: list[dict[str, Any]] = []
    cursor = start_ms
    page = 0

    log.info("Binance  ➜  %s  backfilling from %s to now (1m interval)",
             symbol, settings.BACKFILL_START_DATE)

    while cursor < end_ms:
        page += 1
        try:
            raw_klines = _pull_klines_page(symbol, cursor, end_ms)
        except Exception as e:
            log.error("  %s  page %d  ✗  %s", symbol, page, e)
            break

        if not raw_klines:
            log.info("  %s  page %d  ✓  0 klines (reached end)", symbol, page)
            break

        for kline in raw_klines:
            record = {
                col: kline[i] for i, col in enumerate(settings.KLINE_COLUMNS)
            }
            all_records.append(record)

        last_open_time = raw_klines[-1][0]
        cursor = last_open_time + 1

        log.info("  %s  page %d  ✓  %d klines  [%s … %s]",
                 symbol, page, len(raw_klines),
                 raw_klines[0][0], raw_klines[-1][0])

        if len(raw_klines) < settings.BINANCE_LIMIT:
            break

        time.sleep(0.1)

    return all_records


# ── Main entry point ───────────────────────────────────────────────────────

def pull_api_data() -> dict[str, str]:
    """
    Pull klines for every configured symbol and upload each as Parquet to S3.

    Returns::

        {
            "BTCUSDT": "s3://bucket/klines/symbol=BTCUSDT/date=2026-03-26/data.parquet",
            "ETHUSDT": "s3://bucket/klines/symbol=ETHUSDT/date=2026-03-26/data.parquet",
            ...
        }
    """
    log.info("═══ Task 1: pull_api_data  START ═══")
    results: dict[str, str] = {}

    for i, symbol in enumerate(settings.BINANCE_SYMBOLS, 1):
        log.info("[%d/%d]  %s", i, len(settings.BINANCE_SYMBOLS), symbol)
        try:
            records = _pull_klines_full(symbol)
            if not records:
                log.warning("  ⚠  %s  →  0 klines, skipping upload", symbol)
                results[symbol] = ""
                continue

            s3_path = _upload_parquet_to_s3(records, symbol)
            results[symbol] = s3_path
            log.info("  ✓  %s  →  %d klines  →  %s", symbol, len(records), s3_path)

        except Exception as e:
            log.error("  ✗  %s  →  %s", symbol, e)
            results[symbol] = ""

    total_uploaded = sum(1 for v in results.values() if v)
    log.info("═══ Task 1: pull_api_data  DONE  ═══  (%d/%d symbols uploaded)",
             total_uploaded, len(results))
    return results


if __name__ == "__main__":
    result = pull_api_data()
    for symbol, path in result.items():
        print(f"  {symbol}: {path or 'SKIPPED'}")