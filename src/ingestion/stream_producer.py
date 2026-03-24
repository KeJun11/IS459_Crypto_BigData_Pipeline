from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from dotenv import load_dotenv
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


load_dotenv()

DEFAULT_STREAM_NAME="crypto-ohlcv-1m"
DEFAULT_REGION="us-east-1"
BINANCE_STREAM_BASE_URL = "wss://stream.binance.com:9443/stream?streams="
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEFAULT_SCHEMA_PATH = Path("schemas/kline_1m.schema.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consume Binance kline WebSocket data and publish closed 1-minute candles to Kinesis."
        )
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Symbols to subscribe to.",
    )
    parser.add_argument(
        "--stream-name",
        default=os.getenv("KINESIS_STREAM_NAME", DEFAULT_STREAM_NAME),
        help="Kinesis Data Stream name.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", DEFAULT_REGION)),
        help="AWS region for Kinesis.",
    )
    parser.add_argument(
        "--schema-path",
        type=Path,
        default=DEFAULT_SCHEMA_PATH,
        help="Path to the JSON schema contract used for validation.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate candles but print instead of sending to Kinesis.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        help="Stop after sending this many closed candles.",
    )
    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        help="Stop after this many seconds.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_schema_required_fields(schema_path: Path) -> set[str]:
    payload = json.loads(schema_path.read_text(encoding="utf-8"))
    required = payload.get("required")
    if not isinstance(required, list) or not required:
        raise ValueError(f"Schema file does not contain a usable 'required' list: {schema_path}")
    return set(required)


def build_stream_url(symbols: Iterable[str]) -> str:
    lower_streams = [f"{symbol.lower()}@kline_1m" for symbol in symbols]
    return BINANCE_STREAM_BASE_URL + "/".join(lower_streams)


def extract_kline_payload(message: dict[str, Any]) -> dict[str, Any]:
    if "data" in message:
        return message["data"]
    return message


def normalize_stream_kline(message: dict[str, Any]) -> dict[str, Any] | None:
    payload = extract_kline_payload(message)
    kline = payload.get("k")
    if not isinstance(kline, dict):
        raise ValueError("WebSocket message did not include a kline payload")

    if not bool(kline.get("x")):
        return None

    record = {
        "symbol": str(kline["s"]),
        "open": float(kline["o"]),
        "high": float(kline["h"]),
        "low": float(kline["l"]),
        "close": float(kline["c"]),
        "volume": float(kline["v"]),
        "open_time": int(kline["t"]),
        "close_time": int(kline["T"]),
        "is_closed": bool(kline["x"]),
        "source": "binance_websocket",
        "event_time": int(payload.get("E", kline["T"])),
    }
    return record


def validate_record(record: dict[str, Any], required_fields: set[str]) -> None:
    missing = required_fields.difference(record)
    if missing:
        raise ValueError(f"missing required fields: {sorted(missing)}")
    if record["low"] > record["high"]:
        raise ValueError(f"invalid candle for {record['symbol']}: low > high")
    if record["open_time"] > record["close_time"]:
        raise ValueError(f"invalid candle for {record['symbol']}: open_time > close_time")
    if record["volume"] < 0:
        raise ValueError(f"invalid candle for {record['symbol']}: volume < 0")


class KinesisPublisher:
    def __init__(self, stream_name: str, region: str, dry_run: bool, max_in_flight: int = 20) -> None:
        self.stream_name = stream_name
        self.region = region
        self.dry_run = dry_run
        self.semaphore = asyncio.Semaphore(max_in_flight)
        self.client = None if dry_run else boto3.client(
            "kinesis",
            region_name=region,
            config=Config(
                retries={"max_attempts": 10, "mode": "standard"},
            ),
        )

    async def publish(self, record: dict[str, Any]) -> dict[str, Any] | None:
        async with self.semaphore:
            if self.dry_run:
                logging.info("dry-run record %s", json.dumps(record, separators=(",", ":")))
                return None

            payload = json.dumps(record, separators=(",", ":")).encode("utf-8")
            partition_key = record["symbol"]
            try:
                response = await asyncio.to_thread(
                    self.client.put_record,
                    StreamName=self.stream_name,
                    Data=payload,
                    PartitionKey=partition_key,
                )
            except NoCredentialsError as exc:
                raise RuntimeError(
                    "AWS credentials were not found. Configure them before running the stream producer."
                ) from exc
            except (BotoCoreError, ClientError) as exc:
                raise RuntimeError(f"Kinesis put_record failed: {exc}") from exc

            logging.info(
                "published symbol=%s seq=%s shard=%s",
                partition_key,
                response.get("SequenceNumber"),
                response.get("ShardId"),
            )
            return response


async def produce_stream(args: argparse.Namespace) -> None:
    required_fields = load_schema_required_fields(args.schema_path)
    publisher = KinesisPublisher(
        stream_name=args.stream_name,
        region=args.region,
        dry_run=args.dry_run,
    )
    stream_url = build_stream_url(args.symbols)
    logging.info("connecting to %s", stream_url)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        logging.info("shutdown requested")
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, request_shutdown)
            except NotImplementedError:
                pass

    published_records = 0
    deadline = None
    if args.max_runtime_seconds:
        deadline = loop.time() + args.max_runtime_seconds

    while not stop_event.is_set():
        try:
            async with connect(stream_url, ping_interval=20, ping_timeout=20) as websocket:
                logging.info("websocket connected")
                while not stop_event.is_set():
                    if deadline is not None and loop.time() >= deadline:
                        stop_event.set()
                        break

                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=5)
                    message = json.loads(raw_message)
                    try:
                        record = normalize_stream_kline(message)
                        if record is None:
                            continue
                        validate_record(record, required_fields)
                    except (KeyError, TypeError, ValueError) as exc:
                        logging.warning("dropping malformed stream message: %s", exc)
                        continue

                    await publisher.publish(record)
                    published_records += 1

                    if args.max_records and published_records >= args.max_records:
                        stop_event.set()
                        break
        except asyncio.TimeoutError:
            continue
        except ConnectionClosed as exc:
            logging.warning("websocket disconnected (%s); reconnecting", exc)
            await asyncio.sleep(2)
        except OSError as exc:
            logging.warning("socket error (%s); reconnecting", exc)
            await asyncio.sleep(2)

    logging.info("producer stopped after %s published closed candle(s)", published_records)


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    asyncio.run(produce_stream(args))


if __name__ == "__main__":
    main()
