"""
Task: consume_websocket
=======================
Connects to the Binance WebSocket combined stream for configured symbols
and pushes each CLOSED 1-minute candle as a JSON record to Kinesis Data Stream.

Cold-path flow:
  WebSocket  →  this script  →  Kinesis Data Stream
                                     ↓
                               (Kinesis Firehose — configured in AWS)
                                     ↓
                                  S3 raw  →  Glue catalog

Only closed candles (k.x == True) are forwarded; in-progress ticks are dropped.
"""

import asyncio
import json
import time

import websockets

from pipeline.config import settings
from pipeline.utils.aws_helpers import get_kinesis_client
from pipeline.utils.logger import get_logger

log = get_logger("task.consume_websocket")

_WS_BASE = "wss://stream.binance.com:9443/stream?streams={streams}"


def _build_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s.lower()}@kline_1m" for s in symbols)
    return _WS_BASE.format(streams=streams)


def _parse_closed_candle(msg: dict) -> dict | None:
    """
    Extract OHLCV from a combined-stream message.
    Returns None if the candle is still open.
    """
    candle = msg.get("data", {}).get("k", {})
    if not candle.get("x", False):   # x = is_closed
        return None
    return {
        "symbol":                  candle["s"],
        "source":                  "binance",
        "open_time":               candle["t"],   # epoch ms
        "close_time":              candle["T"],
        "open":                    candle["o"],
        "high":                    candle["h"],
        "low":                     candle["l"],
        "close":                   candle["c"],
        "volume":                  candle["v"],
        "quote_asset_volume":      candle["q"],
        "number_of_trades":        candle["n"],
        "taker_buy_base_volume":   candle["V"],
        "taker_buy_quote_volume":  candle["Q"],
    }


async def _stream(kinesis, symbols: list[str], run_until: float) -> None:
    url = _build_url(symbols)
    log.info("Connecting to Binance combined stream — symbols: %s", symbols)

    reconnect_delay = 1
    max_delay = 60

    while time.monotonic() < run_until:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                reconnect_delay = 1          # reset on successful connect
                log.info("Connected. Waiting for closed candles...")

                while time.monotonic() < run_until:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        log.warning("No message for 30 s — still connected")
                        continue

                    record = _parse_closed_candle(json.loads(raw))
                    if record is None:
                        continue

                    kinesis.put_record(
                        StreamName=settings.KINESIS_STREAM_NAME,
                        Data=json.dumps(record).encode("utf-8"),
                        PartitionKey=record["symbol"],   # keeps symbol ordering
                    )
                    log.info(
                        "→ Kinesis [%s]  close=%s  volume=%s",
                        record["symbol"], record["close"], record["volume"],
                    )

        except (websockets.ConnectionClosed, ConnectionError, OSError) as exc:
            remaining = run_until - time.monotonic()
            if remaining <= 0:
                break
            wait = min(reconnect_delay, remaining)
            log.warning("Connection lost (%s). Reconnecting in %.0fs...", exc, wait)
            await asyncio.sleep(wait)
            reconnect_delay = min(reconnect_delay * 2, max_delay)


def consume_websocket(
    symbols: list[str] | None = None,
    duration_seconds: int | None = None,
) -> None:
    """
    Run the WebSocket consumer.

    Parameters
    ----------
    symbols : list[str], optional
        Symbols to stream. Defaults to settings.STREAM_SYMBOLS.
    duration_seconds : int, optional
        How long to run in seconds.
        0 or None → run indefinitely (use for production / manual runs).
        Positive integer → stop after that many seconds (use in DAG slots).
    """
    symbols = symbols or settings.STREAM_SYMBOLS
    dur = duration_seconds if duration_seconds is not None else settings.STREAM_DURATION_SECONDS
    run_until = (time.monotonic() + dur) if dur else float("inf")

    log.info("═══ consume_websocket  START ═══")
    log.info("Symbols  : %s", symbols)
    log.info("Duration : %s", f"{dur}s" if dur else "indefinite")

    kinesis = get_kinesis_client()
    asyncio.run(_stream(kinesis, symbols, run_until))

    log.info("═══ consume_websocket  DONE  ═══")


if __name__ == "__main__":
    # Indefinite run when invoked directly (Ctrl-C to stop)
    consume_websocket(duration_seconds=0)
