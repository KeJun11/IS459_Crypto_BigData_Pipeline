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

from typing import Any

import requests

from pipeline.config import settings
from pipeline.utils.logger import get_logger

log = get_logger("task1.pull_api_data")


def _pull_klines(symbol: str) -> list[dict[str, Any]]:
    """
    Fetch the latest batch of klines for a single symbol.

    Returns a list of dicts, one per minute candle.
    """
    url = f"{settings.BINANCE_BASE_URL}/klines"
    params = {
        "symbol": symbol,
        "interval": settings.BINANCE_INTERVAL,
        "limit": settings.BINANCE_LIMIT,
    }

    log.info("Binance  ➜  GET klines  %s  interval=%s  limit=%d",
             symbol, settings.BINANCE_INTERVAL, settings.BINANCE_LIMIT)

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw_klines: list[list] = resp.json()

    # Map each positional array → named dict
    records = []
    for kline in raw_klines:
        record = {
            col: kline[i] for i, col in enumerate(settings.KLINE_COLUMNS)
        }
        record["symbol"] = symbol
        records.append(record)

    return records


def pull_api_data() -> dict[str, list[dict[str, Any]]]:
    """
    Pull klines for every configured symbol.

    Returns::

        {
            "BTCUSDT": [ {timestamp, open, high, …}, … ],
            "ETHUSDT": [ … ],
            ...
        }
    """
    log.info("═══ Task 1: pull_api_data  START ═══")
    data: dict[str, list[dict[str, Any]]] = {}

    for symbol in settings.BINANCE_SYMBOLS:
        try:
            records = _pull_klines(symbol)
            data[symbol] = records
            log.info("Binance  ✓  %s  →  %d klines", symbol, len(records))
        except Exception as e:
            log.error("Binance  ✗  %s  →  %s", symbol, e)
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