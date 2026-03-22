"""
Task 1 – pull_api_data
======================
Pulls 1-minute kline (candlestick) data from the Binance public API
for every symbol in settings.BINANCE_SYMBOLS.

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


SOURCES = {
    "binance": {
        "base_url": settings.BINANCE_BASE_URL,
        "symbols": settings.BINANCE_SYMBOLS,
        "limit": settings.BINANCE_LIMIT,
        "sleep": 0.1,   # seconds between requests (conservative)
    },
    "mexc": {
        "base_url": settings.MEXC_BASE_URL,
        "symbols": settings.MEXC_SYMBOLS,
        "limit": settings.MEXC_LIMIT,
        "sleep": 0.2,   # MEXC has tighter per-second limits
    },
}



def _pull_klines_page(
    base_url: str,
    symbol: str,
    interval: str,
    limit: int,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[list]:
    """Fetch a single page of klines from Binance or MEXC."""
    url = f"{base_url}/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _pull_klines_full(
    source_name: str,
    base_url: str,
    symbol: str,
    limit: int,
    sleep_sec: float,
) -> list[dict[str, Any]]:
    """
    Pull all daily klines for a symbol going back BACKFILL_DAYS.
    Paginates automatically using startTime/endTime.
    """
    interval = settings.BINANCE_INTERVAL
    end_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    start_ms = end_ms - (settings.BACKFILL_DAYS * 24 * 60 * 60 * 1000)

    all_records: list[dict[str, Any]] = []
    cursor = start_ms
    page = 0

    while cursor < end_ms:
        page += 1
        try:
            raw = _pull_klines_page(
                base_url=base_url,
                symbol=symbol,
                interval=interval,
                limit=limit,
                start_time=cursor,
                end_time=end_ms,
            )
        except Exception as e:
            log.error("  %s  ✗  %s  page %d  →  %s", source_name, symbol, page, e)
            break

        if not raw:
            break

        for kline in raw:
            record = {}
            for i, col in enumerate(settings.KLINE_COLUMNS):
                record[col] = kline[i] if i < len(kline) else None
            record["symbol"] = symbol
            record["source"] = source_name
            all_records.append(record)

        # Move cursor past the last kline's open_time to avoid duplicates
        last_open_time = raw[-1][0]
        cursor = last_open_time + 1

        # If we got fewer than limit, we've reached the end
        if len(raw) < limit:
            break

        time.sleep(sleep_sec)

    return all_records


def pull_api_data() -> dict[str, dict[str, list[dict[str, Any]]]]:
    """
    Pull klines from all enabled sources.

    Returns::

        {
            "binance": {
                "BTCUSDT": [ {timestamp, open, …, source: "binance"}, … ],
                ...
            },
            "mexc": {
                "BTCUSDT": [ {timestamp, open, …, source: "mexc"}, … ],
                ...
            },
        }
    """
    log.info("═══ Task 1: pull_api_data  START ═══")
    log.info("Interval: %s | Backfill: %d days | Sources: %s",
             settings.BINANCE_INTERVAL, settings.BACKFILL_DAYS,
             settings.ENABLED_SOURCES)

    all_data: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for source_name in settings.ENABLED_SOURCES:
        cfg = SOURCES[source_name]
        log.info("── Source: %s  (%d symbols) ──", source_name, len(cfg["symbols"]))

        source_data: dict[str, list[dict[str, Any]]] = {}

        for i, symbol in enumerate(cfg["symbols"], 1):
            log.info("[%d/%d]  %s  ➜  %s",
                     i, len(cfg["symbols"]), source_name, symbol)

            records = _pull_klines_full(
                source_name=source_name,
                base_url=cfg["base_url"],
                symbol=symbol,
                limit=cfg["limit"],
                sleep_sec=cfg["sleep"],
            )
            source_data[symbol] = records

            if records:
                log.info("  ✓  %d klines  [%s → %s]",
                         len(records), records[0]["timestamp"], records[-1]["timestamp"])
            else:
                log.warning("  ✗  EMPTY")

        all_data[source_name] = source_data

        total = sum(len(v) for v in source_data.values())
        log.info("── %s DONE  (%d symbols, %d total klines) ──",
                 source_name, len(source_data), total)

    grand_total = sum(
        len(recs)
        for src in all_data.values()
        for recs in src.values()
    )
    log.info("═══ Task 1: pull_api_data  DONE  ═══  (%d total klines across all sources)",
             grand_total)
    return all_data


if __name__ == "__main__":
    import json

    result = pull_api_data()
    for source, symbols in result.items():
        print(f"\n{'='*40}")
        print(f"  Source: {source}")
        print(f"{'='*40}")
        for symbol, records in symbols.items():
            if records:
                print(f"  {symbol}: {len(records)} klines")
            else:
                print(f"  {symbol}: EMPTY")