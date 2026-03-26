from __future__ import annotations

import argparse
import os
import zipfile
from pathlib import Path

import boto3
from dotenv import load_dotenv


load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Package and upload EMR Serverless Spark batch artifacts to S3."
    )
    parser.add_argument("--bucket", default=os.getenv("BATCH_S3_BUCKET", ""))
    parser.add_argument("--prefix", default="artifacts/emr/phase6")
    parser.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "dist" / "emr_serverless"))
    parser.add_argument("--env-file", default=str(REPO_ROOT / ".env"))
    parser.add_argument("--skip-env-update", action="store_true")
    return parser.parse_args()


def ensure_bucket(bucket: str) -> str:
    if not bucket:
        raise ValueError("Missing --bucket and BATCH_S3_BUCKET is not set.")
    return bucket


def build_src_zip(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / "src.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted((REPO_ROOT / "src").rglob("*.py")):
            archive.write(path, arcname=path.relative_to(REPO_ROOT))
    return zip_path


def upload_file(s3_client, bucket: str, key: str, local_path: Path) -> str:
    s3_client.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"


def set_env_value(env_path: Path, name: str, value: str) -> None:
    if not env_path.exists():
        env_path.write_text("", encoding="utf-8")
    lines = env_path.read_text(encoding="utf-8").splitlines()
    rendered = f"{name}={value}"
    for index, line in enumerate(lines):
        if line.startswith(f"{name}="):
            lines[index] = rendered
            break
    else:
        lines.append(rendered)
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    bucket = ensure_bucket(args.bucket)
    prefix = args.prefix.strip().strip("/")
    output_dir = Path(args.output_dir)
    env_path = Path(args.env_file)

    kaggle_script = REPO_ROOT / "src" / "jobs" / "kaggle_csv_to_bronze.py"
    bronze_script = REPO_ROOT / "src" / "jobs" / "bronze_to_silver_batch.py"
    gold_script = REPO_ROOT / "src" / "jobs" / "silver_to_gold_batch.py"
    src_zip = build_src_zip(output_dir)

    s3_client = boto3.client("s3", region_name=args.region)
    kaggle_uri = upload_file(s3_client, bucket, f"{prefix}/jobs/kaggle_csv_to_bronze.py", kaggle_script)
    bronze_uri = upload_file(s3_client, bucket, f"{prefix}/jobs/bronze_to_silver_batch.py", bronze_script)
    gold_uri = upload_file(s3_client, bucket, f"{prefix}/jobs/silver_to_gold_batch.py", gold_script)
    src_zip_uri = upload_file(s3_client, bucket, f"{prefix}/packages/src.zip", src_zip)

    if not args.skip_env_update:
        set_env_value(env_path, "KAGGLE_TO_BRONZE_ENTRYPOINT", kaggle_uri)
        set_env_value(env_path, "AIRFLOW_BRONZE_TO_SILVER_ENTRYPOINT", bronze_uri)
        set_env_value(env_path, "AIRFLOW_SILVER_TO_GOLD_ENTRYPOINT", gold_uri)
        set_env_value(env_path, "AIRFLOW_EMR_PY_FILES", src_zip_uri)

    print(f"kaggle_to_bronze_entrypoint={kaggle_uri}")
    print(f"bronze_to_silver_entrypoint={bronze_uri}")
    print(f"silver_to_gold_entrypoint={gold_uri}")
    print(f"emr_py_files={src_zip_uri}")


if __name__ == "__main__":
    main()
