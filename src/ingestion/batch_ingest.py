from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv


load_dotenv()

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
DEFAULT_INTERVAL = "1m"
DEFAULT_LIMIT = 1000
DEFAULT_MAX_WEIGHT_PER_MINUTE = 1200
INTERVAL_TO_MS = {
    "1m": 60_000,
}


@dataclass
class IngestionSummary:
    symbol: str
    rows_written: int = 0
    files_written: int = 0
    first_open_time: int | None = None
    last_open_time: int | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pull historical Binance klines for the Bronze layer. "
            "Use --sample-only for a network-free smoke test."
        )
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        help="Symbols to pull from Binance.",
    )
    parser.add_argument(
        "--interval",
        default=DEFAULT_INTERVAL,
        choices=sorted(INTERVAL_TO_MS.keys()),
        help="Kline interval. Only 1m is implemented for this project.",
    )
    parser.add_argument(
        "--start-date",
        help="UTC start date in YYYY-MM-DD format. Defaults to --days-back from today.",
    )
    parser.add_argument(
        "--end-date",
        help="UTC end date in YYYY-MM-DD format. Defaults to today UTC.",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="Used when --start-date is not provided.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximum klines per request.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/bronze/rest"),
        help="Local output root for partitioned files.",
    )
    parser.add_argument(
        "--output-format",
        choices=["parquet", "jsonl"],
        default="parquet",
        help="Parquet matches the roadmap; jsonl is useful for smoke tests without pyarrow.",
    )
    parser.add_argument(
        "--resume-state",
        type=Path,
        default=Path("data/state/batch_ingest_state.json"),
        help="JSON file storing the next timestamp to pull for each symbol.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the saved state if present.",
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Upload written files to S3 after local write succeeds.",
    )
    parser.add_argument(
        "--s3-bucket",
        default=os.getenv("BATCH_S3_BUCKET") or os.getenv("S3_BUCKET"),
        help="S3 bucket for uploads when --upload-s3 is used.",
    )
    parser.add_argument(
        "--s3-prefix",
        default=(
            os.getenv("BATCH_S3_PREFIX")
            or os.getenv("S3_PREFIX")
            or "bronze/rest/binance-klines"
        ),
        help="S3 prefix for uploads when --upload-s3 is used.",
    )
    parser.add_argument(
        "--limit-pages",
        type=int,
        help="Cap the number of request pages per symbol. Useful for testing.",
    )
    parser.add_argument(
        "--sample-only",
        action="store_true",
        help="Generate synthetic klines locally instead of calling Binance.",
    )
    return parser.parse_args()


def parse_utc_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)


def datetime_to_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def ms_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def compute_date_window(args: argparse.Namespace) -> tuple[int, int]:
    if args.end_date:
        end_dt = parse_utc_date(args.end_date) + timedelta(days=1) - timedelta(milliseconds=1)
    else:
        now = datetime.now(tz=UTC)
        end_dt = now.replace(hour=23, minute=59, second=59, microsecond=999000)

    if args.start_date:
        start_dt = parse_utc_date(args.start_date)
    else:
        start_dt = (end_dt - timedelta(days=args.days_back)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    if start_dt > end_dt:
        raise ValueError("start date must be before end date")

    return datetime_to_ms(start_dt), datetime_to_ms(end_dt)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"symbols": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def build_s3_key(prefix: str, relative_path: Path) -> str:
    clean_prefix = prefix.strip("/")
    suffix = relative_path.as_posix().lstrip("/")
    return f"{clean_prefix}/{suffix}" if clean_prefix else suffix


def maybe_import_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Parquet output requires pyarrow. Install it or rerun with --output-format jsonl."
        ) from exc
    return pa, pq


def validate_record(record: dict[str, Any]) -> None:
    required_fields = {
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_time",
        "close_time",
        "is_closed",
    }
    missing = required_fields.difference(record)
    if missing:
        raise ValueError(f"missing required fields: {sorted(missing)}")
    if record["low"] > record["high"]:
        raise ValueError(f"invalid candle for {record['symbol']}: low > high")
    if record["open_time"] > record["close_time"]:
        raise ValueError(f"invalid candle for {record['symbol']}: open_time > close_time")


def normalize_rest_kline(symbol: str, raw_kline: list[Any]) -> dict[str, Any]:
    record = {
        "symbol": symbol,
        "open": float(raw_kline[1]),
        "high": float(raw_kline[2]),
        "low": float(raw_kline[3]),
        "close": float(raw_kline[4]),
        "volume": float(raw_kline[5]),
        "open_time": int(raw_kline[0]),
        "close_time": int(raw_kline[6]),
        "is_closed": True,
        "source": "binance_rest",
        "ingested_at": datetime_to_ms(datetime.now(tz=UTC)),
    }
    validate_record(record)
    return record


def fetch_binance_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    timeout_seconds: int = 30,
) -> tuple[list[list[Any]], dict[str, str]]:
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit,
    }
    headers = {
        "User-Agent": "is459-crypto-batch-ingest/0.1",
    }
    api_key = os.getenv("BINANCE_API_KEY")
    if api_key:
        headers["X-MBX-APIKEY"] = api_key

    request = Request(f"{BINANCE_KLINES_URL}?{urlencode(params)}", headers=headers)
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
        response_headers = {key.lower(): value for key, value in response.headers.items()}
        return payload, response_headers


def fetch_with_retries(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    max_attempts: int = 5,
) -> tuple[list[list[Any]], dict[str, str]]:
    for attempt in range(1, max_attempts + 1):
        try:
            return fetch_binance_klines(symbol, interval, start_ms, end_ms, limit)
        except HTTPError as exc:
            if exc.code not in {418, 429, 500, 502, 503, 504} or attempt == max_attempts:
                raise
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            sleep_seconds = float(retry_after) if retry_after else min(2**attempt, 30)
            print(
                f"[{symbol}] HTTP {exc.code} on attempt {attempt}/{max_attempts}; "
                f"sleeping {sleep_seconds:.1f}s before retry"
            )
            time.sleep(sleep_seconds)
        except URLError:
            if attempt == max_attempts:
                raise
            sleep_seconds = min(2**attempt, 10)
            print(
                f"[{symbol}] network error on attempt {attempt}/{max_attempts}; "
                f"sleeping {sleep_seconds:.1f}s before retry"
            )
            time.sleep(sleep_seconds)
    raise RuntimeError("unreachable retry state")


def apply_dynamic_backoff(headers: dict[str, str], max_weight_per_minute: int) -> None:
    weight_header = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbn-used-weight")
    if not weight_header:
        return

    used_weight = int(weight_header)
    pressure = used_weight / max_weight_per_minute
    if pressure >= 0.95:
        sleep_seconds = 10.0
    elif pressure >= 0.90:
        sleep_seconds = 5.0
    elif pressure >= 0.80:
        sleep_seconds = 1.5
    else:
        sleep_seconds = 0.0

    if sleep_seconds > 0:
        print(
            f"Rate-limit pressure is {pressure:.0%} "
            f"({used_weight}/{max_weight_per_minute}); sleeping {sleep_seconds:.1f}s"
        )
        time.sleep(sleep_seconds)


def generate_sample_klines(
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    interval_ms: int,
) -> tuple[list[list[Any]], dict[str, str]]:
    rows: list[list[Any]] = []
    current_ms = start_ms
    index = 0
    while current_ms <= end_ms and len(rows) < limit:
        base_price = 65_000 + (index * 3.5)
        close_price = base_price + 1.25
        high_price = close_price + 0.75
        low_price = base_price - 0.75
        rows.append(
            [
                current_ms,
                f"{base_price:.2f}",
                f"{high_price:.2f}",
                f"{low_price:.2f}",
                f"{close_price:.2f}",
                f"{10 + index * 0.1:.4f}",
                current_ms + interval_ms - 1,
                "0",
                42,
                "0",
                "0",
                "0",
            ]
        )
        current_ms += interval_ms
        index += 1
    return rows, {"x-mbx-used-weight-1m": "2"}


def partition_records(records: list[dict[str, Any]]) -> dict[tuple[str, int, int], list[dict[str, Any]]]:
    grouped: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        open_dt = ms_to_datetime(record["open_time"])
        key = (record["symbol"], open_dt.year, open_dt.month)
        grouped[key].append(record)
    return grouped


def write_partition(
    records: list[dict[str, Any]],
    output_root: Path,
    output_format: str,
) -> list[Path]:
    paths: list[Path] = []
    grouped_records = partition_records(records)
    for (symbol, year, month), partition_rows in grouped_records.items():
        partition_rows.sort(key=lambda row: row["open_time"])
        partition_dir = (
            output_root
            / f"symbol={symbol}"
            / f"year={year:04d}"
            / f"month={month:02d}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        first_open = partition_rows[0]["open_time"]
        last_open = partition_rows[-1]["open_time"]
        file_stem = f"candles_{first_open}_{last_open}"

        if output_format == "parquet":
            pa, pq = maybe_import_pyarrow()
            output_path = partition_dir / f"{file_stem}.parquet"
            table = pa.Table.from_pylist(partition_rows)
            pq.write_table(table, output_path, compression="snappy")
        else:
            output_path = partition_dir / f"{file_stem}.jsonl"
            with output_path.open("w", encoding="utf-8") as handle:
                for row in partition_rows:
                    handle.write(json.dumps(row))
                    handle.write("\n")

        paths.append(output_path)
    return paths


def upload_files_to_s3(
    files: list[Path],
    output_root: Path,
    bucket: str,
    prefix: str,
) -> list[str]:
    s3 = boto3.client("s3")
    uploaded_keys: list[str] = []
    for path in files:
        relative_path = path.relative_to(output_root)
        key = build_s3_key(prefix, relative_path)
        try:
            s3.upload_file(str(path), bucket, key)
        except NoCredentialsError as exc:
            raise RuntimeError(
                "AWS credentials were not found. Configure AWS_ACCESS_KEY_ID / "
                "AWS_SECRET_ACCESS_KEY, an AWS profile, or an instance role before using --upload-s3."
            ) from exc
        print(f"Uploaded s3://{bucket}/{key}")
        uploaded_keys.append(key)
    return uploaded_keys


def update_summary(summary: IngestionSummary, records: list[dict[str, Any]], files_written: int) -> None:
    if not records:
        return
    summary.rows_written += len(records)
    summary.files_written += files_written
    first_open = records[0]["open_time"]
    last_open = records[-1]["open_time"]
    summary.first_open_time = (
        first_open if summary.first_open_time is None else min(summary.first_open_time, first_open)
    )
    summary.last_open_time = (
        last_open if summary.last_open_time is None else max(summary.last_open_time, last_open)
    )


def print_summary(summary: IngestionSummary) -> None:
    if summary.rows_written == 0:
        print(f"[{summary.symbol}] no rows written")
        return
    first_dt = ms_to_datetime(summary.first_open_time).isoformat() if summary.first_open_time else "n/a"
    last_dt = ms_to_datetime(summary.last_open_time).isoformat() if summary.last_open_time else "n/a"
    print(
        f"[{summary.symbol}] wrote {summary.rows_written} rows across {summary.files_written} files "
        f"covering {first_dt} -> {last_dt}"
    )


def ingest_symbol(
    symbol: str,
    args: argparse.Namespace,
    state: dict[str, Any],
    requested_start_ms: int,
    requested_end_ms: int,
) -> IngestionSummary:
    interval_ms = INTERVAL_TO_MS[args.interval]
    symbol_state = state.setdefault("symbols", {}).get(symbol, {})
    next_start_ms = requested_start_ms
    if args.resume and symbol_state.get("next_start_ms"):
        next_start_ms = max(requested_start_ms, int(symbol_state["next_start_ms"]))

    summary = IngestionSummary(symbol=symbol)
    pages_seen = 0

    while next_start_ms <= requested_end_ms:
        pages_seen += 1
        page_end_ms = min(
            requested_end_ms,
            next_start_ms + ((args.limit - 1) * interval_ms),
        )

        if args.sample_only:
            raw_rows, headers = generate_sample_klines(
                symbol=symbol,
                start_ms=next_start_ms,
                end_ms=page_end_ms,
                limit=args.limit,
                interval_ms=interval_ms,
            )
        else:
            raw_rows, headers = fetch_with_retries(
                symbol=symbol,
                interval=args.interval,
                start_ms=next_start_ms,
                end_ms=page_end_ms,
                limit=args.limit,
            )

        if not raw_rows:
            break

        records = [normalize_rest_kline(symbol, row) for row in raw_rows]
        written_files = write_partition(records, args.output_root, args.output_format)
        if args.upload_s3:
            if not args.s3_bucket:
                raise ValueError("--upload-s3 requires --s3-bucket or S3_BUCKET in the environment")
            upload_files_to_s3(written_files, args.output_root, args.s3_bucket, args.s3_prefix)

        update_summary(summary, records, len(written_files))

        last_open_ms = records[-1]["open_time"]
        next_start_ms = last_open_ms + interval_ms
        state["symbols"][symbol] = {
            "next_start_ms": next_start_ms,
            "updated_at": datetime.now(tz=UTC).isoformat(),
        }
        save_state(args.resume_state, state)

        apply_dynamic_backoff(headers, DEFAULT_MAX_WEIGHT_PER_MINUTE)

        if args.limit_pages and pages_seen >= args.limit_pages:
            break

        if len(raw_rows) < args.limit:
            break

    return summary


def main() -> None:
    args = parse_args()
    requested_start_ms, requested_end_ms = compute_date_window(args)
    state = load_state(args.resume_state)

    for symbol in args.symbols:
        summary = ingest_symbol(
            symbol=symbol,
            args=args,
            state=state,
            requested_start_ms=requested_start_ms,
            requested_end_ms=requested_end_ms,
        )
        print_summary(summary)


if __name__ == "__main__":
    main()
