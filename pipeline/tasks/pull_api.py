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

This task converts each array to a dict using settings.KLINE_COLUMNS.
"""

import time
import datetime as dt
from typing import Any

import requests

from pipeline.config import settings
from pipeline.utils.logger import get_logger

log = get_logger("task1.pull_api_data")


def _parse_start_time() -> int:
    """
    Parse BACKFILL_START_DATE (format: YYYY-MM-DD) and return milliseconds since epoch.
    """
    date_obj = dt.datetime.strptime(settings.BACKFILL_START_DATE, "%Y-%m-%d")
    date_obj = date_obj.replace(tzinfo=dt.timezone.utc)
    return int(date_obj.timestamp() * 1000)


def _pull_klines_page(symbol: str, start_time: int, end_time: int) -> list[list]:
    """
    Fetch a single page of klines between start_time and end_time (milliseconds).
    Returns a list of kline arrays.
    """
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
    """
    Fetch all klines for a symbol from BACKFILL_START_DATE to now.
    Paginates automatically using startTime/endTime.
    """
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
            record["symbol"] = symbol
            all_records.append(record)

        # Move cursor to after the last kline's open_time to avoid duplicates
        last_open_time = raw_klines[-1][0]
        cursor = last_open_time + 1

        log.info("  %s  page %d  ✓  %d klines  [%s … %s]",
                 symbol, page, len(raw_klines), raw_klines[0][0], raw_klines[-1][0])

        # If we got fewer than limit, we've reached the end
        if len(raw_klines) < settings.BINANCE_LIMIT:
            break

        time.sleep(0.1)  # Be respectful to API

    return all_records


def pull_api_data() -> dict[str, list[dict[str, Any]]]:
    """
    Pull klines for every configured symbol from backfill start date to now.

    Returns::

        {
            "BTCUSDT": [ {timestamp, open, high, …}, … ],
            "ETHUSDT": [ … ],
            ...
        }
    """
    log.info("═══ Task 1: pull_api_data  START ═══")
    data: dict[str, list[dict[str, Any]]] = {}

    for i, symbol in enumerate(settings.BINANCE_SYMBOLS, 1):
        log.info("[%d/%d]  %s", i, len(settings.BINANCE_SYMBOLS), symbol)
        try:
            records = _pull_klines_full(symbol)
            data[symbol] = records
            log.info("  ✓  %s  →  %d klines total", symbol, len(records))
        except Exception as e:
            log.error("  ✗  %s  →  %s", symbol, e)
            data[symbol] = []

    total = sum(len(v) for v in data.values())
    log.info("═══ Task 1: pull_api_data  DONE  ═══  (%d symbols, %d total klines)",
             len(data), total)
    return data


if __name__ == "__main__":
    import json

    result = pull_api_data()
    for symbol, records in result.items():
        if records:
            print(f"\n  {symbol}: {len(records)} klines")
            print(f"    first → {json.dumps(records[0], default=str)[:150]}…")
            print(f"    last  → {json.dumps(records[-1], default=str)[:150]}…")
        else:
            print(f"\n  {symbol}: EMPTY")