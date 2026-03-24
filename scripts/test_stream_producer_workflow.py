from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv


load_dotenv()


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from src.ingestion.stream_producer import (  # noqa: E402
    DEFAULT_SYMBOLS,
    build_stream_url,
    load_schema_required_fields,
    normalize_stream_kline,
    validate_record,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test the real-time stream producer workflow."
    )
    parser.add_argument(
        "--mode",
        choices=["sample", "live"],
        default="sample",
        help="sample validates parsing locally; live runs the websocket producer.",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Symbols to subscribe to or validate.",
    )
    parser.add_argument(
        "--schema-path",
        type=Path,
        default=Path("schemas/kline_1m.schema.json"),
        help="Schema path used by the producer.",
    )
    parser.add_argument(
        "--stream-name",
        default=os.getenv("KINESIS_STREAM_NAME", "nothing"),
        help="Kinesis stream name for live publish mode.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
        help="AWS region for live publish mode.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=3,
        help="Number of closed candles to capture during live mode.",
    )
    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=60,
        help="Maximum runtime for live mode.",
    )
    parser.add_argument(
        "--publish-kinesis",
        action="store_true",
        help="In live mode, publish to Kinesis instead of dry-run logging only.",
    )
    return parser.parse_args()


def build_sample_messages(symbol: str) -> tuple[dict, dict]:
    open_message = {
        "stream": f"{symbol.lower()}@kline_1m",
        "data": {
            "E": 1772323220000,
            "k": {
                "t": 1772323200000,
                "T": 1772323259999,
                "s": symbol,
                "o": "66973.26",
                "c": "66980.00",
                "h": "67000.00",
                "l": "66950.00",
                "v": "3.50000",
                "x": False,
            },
        },
    }
    closed_message = {
        "stream": f"{symbol.lower()}@kline_1m",
        "data": {
            "E": 1772323259999,
            "k": {
                "t": 1772323200000,
                "T": 1772323259999,
                "s": symbol,
                "o": "66973.26",
                "c": "66998.58",
                "h": "67029.25",
                "l": "66967.18",
                "v": "9.19306",
                "x": True,
            },
        },
    }
    return open_message, closed_message


def run_sample_test(args: argparse.Namespace) -> None:
    required_fields = load_schema_required_fields(REPO_ROOT / args.schema_path)
    stream_url = build_stream_url(args.symbols)
    if not stream_url.startswith("wss://"):
        raise RuntimeError("Generated stream URL is invalid")

    for symbol in args.symbols:
        open_message, closed_message = build_sample_messages(symbol)

        open_record = normalize_stream_kline(open_message)
        if open_record is not None:
            raise RuntimeError("Open candle should not be emitted by the producer")

        closed_record = normalize_stream_kline(closed_message)
        if closed_record is None:
            raise RuntimeError("Closed candle should have been emitted by the producer")

        validate_record(closed_record, required_fields)

    print(f"Verified schema validation and closed-candle filtering for {len(args.symbols)} symbol(s)")
    print(f"Verified combined stream URL: {stream_url}")
    print("Stream producer sample smoke test passed")


def run_subprocess(command: list[str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="")
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {' '.join(command)}")
    return result


def ensure_publish_prereqs(args: argparse.Namespace) -> None:
    if boto3.Session().get_credentials() is None:
        raise RuntimeError(
            "AWS credentials were not found. Configure credentials before using --publish-kinesis."
        )


def run_live_test(args: argparse.Namespace) -> None:
    if args.publish_kinesis:
        ensure_publish_prereqs(args)

    command = [
        sys.executable,
        str(REPO_ROOT / "src" / "ingestion" / "stream_producer.py"),
        "--symbols",
        *args.symbols,
        "--stream-name",
        args.stream_name,
        "--region",
        args.region,
        "--max-records",
        str(args.max_records),
        "--max-runtime-seconds",
        str(args.max_runtime_seconds),
        "--log-level",
        "INFO",
    ]
    if not args.publish_kinesis:
        command.append("--dry-run")

    result = run_subprocess(command)
    combined_output = f"{result.stdout}\n{result.stderr}"

    if "websocket connected" not in combined_output:
        raise RuntimeError("Live test did not reach a websocket-connected state")

    if args.publish_kinesis:
        if "published symbol=" not in combined_output:
            raise RuntimeError("Live publish test did not observe any Kinesis publish log entries")
        print("Stream producer live publish test passed")
    else:
        if "dry-run record" not in combined_output:
            raise RuntimeError("Live dry-run test did not observe any validated closed-candle records")
        print("Stream producer live dry-run test passed")


def main() -> None:
    args = parse_args()
    if args.mode == "sample":
        run_sample_test(args)
    else:
        run_live_test(args)


if __name__ == "__main__":
    main()
