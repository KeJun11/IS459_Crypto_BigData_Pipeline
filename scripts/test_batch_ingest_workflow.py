from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv


load_dotenv()


REPO_ROOT = Path(__file__).resolve().parents[1]
BATCH_INGEST_SCRIPT = REPO_ROOT / "src" / "ingestion" / "batch_ingest.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a smoke test for the batch ingestion workflow."
    )
    parser.add_argument(
        "--mode",
        choices=["sample", "live"],
        default="sample",
        help="sample avoids Binance; live performs a small real REST pull.",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT"],
        help="Symbols to test.",
    )
    parser.add_argument(
        "--start-date",
        default="2026-03-01",
        help="UTC start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        default="2026-03-01",
        help="UTC end date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Rows per request page for the smoke test.",
    )
    parser.add_argument(
        "--limit-pages",
        type=int,
        default=1,
        help="Page cap for the smoke test.",
    )
    parser.add_argument(
        "--output-format",
        choices=["parquet", "jsonl"],
        default="parquet",
        help="parquet is the normal path; jsonl is useful if debugging without pyarrow.",
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Also upload the generated files to S3 and verify the keys exist.",
    )
    parser.add_argument(
        "--s3-bucket",
        default=os.getenv("BATCH_S3_BUCKET") or os.getenv("S3_BUCKET"),
        help="S3 bucket override. Defaults to BATCH_S3_BUCKET or S3_BUCKET from .env.",
    )
    parser.add_argument(
        "--s3-prefix",
        default=os.getenv("BATCH_S3_PREFIX") or os.getenv("S3_PREFIX"),
        help="S3 prefix override. Defaults to BATCH_S3_PREFIX or a unique test prefix per run.",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep local test artifacts under data/test_runs after the test passes.",
    )
    parser.add_argument(
        "--keep-s3",
        action="store_true",
        help="Keep S3 test files after verification passes.",
    )
    return parser.parse_args()


def build_s3_key(prefix: str, relative_path: Path) -> str:
    clean_prefix = prefix.strip("/")
    suffix = relative_path.as_posix().lstrip("/")
    return f"{clean_prefix}/{suffix}" if clean_prefix else suffix


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
        print(result.stderr, file=sys.stderr, end="")
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {' '.join(command)}")
    return result


def verify_local_files(output_root: Path, output_format: str) -> list[Path]:
    suffix = "*.parquet" if output_format == "parquet" else "*.jsonl"
    files = sorted(output_root.rglob(suffix))
    if not files:
        raise RuntimeError(f"No {output_format} files were written under {output_root}")

    total_rows = 0
    for file_path in files:
        if output_format == "parquet":
            total_rows += pq.ParquetFile(file_path).metadata.num_rows
        else:
            total_rows += sum(1 for _ in file_path.open("r", encoding="utf-8"))

    if total_rows == 0:
        raise RuntimeError("Output files were created but contain zero rows")

    print(f"Verified {len(files)} local {output_format} file(s) with {total_rows} total row(s)")
    return files


def verify_resume_behavior(
    base_command: list[str],
    output_root: Path,
    output_format: str,
    state_path: Path,
    symbols: list[str],
) -> None:
    before_files = sorted(output_root.rglob("*.parquet" if output_format == "parquet" else "*.jsonl"))
    before_state = json.loads(state_path.read_text(encoding="utf-8"))
    run_subprocess([*base_command, "--resume"])
    after_files = sorted(output_root.rglob("*.parquet" if output_format == "parquet" else "*.jsonl"))
    after_state = json.loads(state_path.read_text(encoding="utf-8"))

    if not set(before_files).issubset(set(after_files)):
        raise RuntimeError("Resume run replaced or removed existing output files unexpectedly")

    for symbol in symbols:
        before_next_start = before_state["symbols"][symbol]["next_start_ms"]
        after_next_start = after_state["symbols"][symbol]["next_start_ms"]
        if after_next_start <= before_next_start:
            raise RuntimeError(f"Resume run did not advance state for {symbol}")

    print("Verified resume behavior: rerun advanced state without overwriting prior output")


def verify_state_file(state_path: Path, symbols: list[str]) -> None:
    if not state_path.exists():
        raise RuntimeError(f"Resume state file was not created: {state_path}")
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    missing_symbols = [symbol for symbol in symbols if symbol not in payload.get("symbols", {})]
    if missing_symbols:
        raise RuntimeError(f"State file is missing symbols: {missing_symbols}")
    print(f"Verified state file at {state_path}")


def verify_s3_upload(
    files: list[Path],
    output_root: Path,
    bucket: str,
    prefix: str,
) -> list[str]:
    s3 = boto3.client("s3")
    uploaded_keys: list[str] = []
    for file_path in files:
        key = build_s3_key(prefix, file_path.relative_to(output_root))
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except NoCredentialsError as exc:
            raise RuntimeError(
                "AWS credentials were not found for the S3 verification step."
            ) from exc
        uploaded_keys.append(key)
    print(f"Verified {len(uploaded_keys)} object(s) in s3://{bucket}/{prefix}")
    return uploaded_keys


def delete_s3_objects(bucket: str, keys: list[str]) -> None:
    if not keys:
        return
    s3 = boto3.client("s3")
    chunks = [keys[index : index + 1000] for index in range(0, len(keys), 1000)]
    for chunk in chunks:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )


def main() -> None:
    args = parse_args()
    run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    test_root = REPO_ROOT / "data" / "test_runs" / f"batch_ingest_{run_id}"
    output_root = test_root / "bronze"
    state_path = test_root / "state" / "batch_ingest_state.json"
    s3_bucket = args.s3_bucket
    s3_prefix = args.s3_prefix or f"bronze/rest/tests/batch_ingest/{run_id}"

    command = [
        sys.executable,
        str(BATCH_INGEST_SCRIPT),
        "--symbols",
        *args.symbols,
        "--start-date",
        args.start_date,
        "--end-date",
        args.end_date,
        "--limit",
        str(args.limit),
        "--limit-pages",
        str(args.limit_pages),
        "--output-root",
        str(output_root),
        "--output-format",
        args.output_format,
        "--resume-state",
        str(state_path),
    ]

    if args.mode == "sample":
        command.append("--sample-only")

    if args.upload_s3:
        if not s3_bucket:
            raise RuntimeError("S3 bucket is required for --upload-s3")
        if boto3.Session().get_credentials() is None:
            raise RuntimeError(
                "AWS credentials were not found. Configure credentials locally before running --upload-s3."
            )
        command.extend(
            [
                "--upload-s3",
                "--s3-bucket",
                s3_bucket,
                "--s3-prefix",
                s3_prefix,
            ]
        )

    if test_root.exists():
        shutil.rmtree(test_root)

    uploaded_keys: list[str] = []
    try:
        run_subprocess(command)
        files = verify_local_files(output_root, args.output_format)
        verify_state_file(state_path, args.symbols)

        if args.upload_s3:
            uploaded_keys = verify_s3_upload(files, output_root, s3_bucket, s3_prefix)

        verify_resume_behavior(command, output_root, args.output_format, state_path, args.symbols)
        print("Batch ingestion workflow smoke test passed")
    finally:
        if args.upload_s3 and uploaded_keys and not args.keep_s3:
            delete_s3_objects(s3_bucket, uploaded_keys)
            print(f"Deleted S3 smoke-test object(s) from s3://{s3_bucket}/{s3_prefix}")
        if test_root.exists() and not args.keep_local:
            shutil.rmtree(test_root)
            print(f"Deleted local smoke-test files from {test_root}")


if __name__ == "__main__":
    main()
