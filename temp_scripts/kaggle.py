import os
from pathlib import Path

import boto3
import kagglehub
from dotenv import load_dotenv


load_dotenv()


DATASET_HANDLE = os.getenv("KAGGLE_DATASET", "kaanxtr/btc-price-1m")
S3_BUCKET = os.environ["BATCH_S3_BUCKET"]
S3_PREFIX = os.getenv("S3_PREFIX", "bronze/kaggle/btc-price-1m")
FORCE_DOWNLOAD = os.getenv("KAGGLE_FORCE_DOWNLOAD", "false").lower() == "true"


def iter_downloaded_files(path: Path):
    if path.is_file():
        yield path, Path(path.name)
        return

    for file_path in path.rglob("*"):
        if file_path.is_file():
            yield file_path, file_path.relative_to(path)


def build_s3_key(prefix: str, relative_path: Path) -> str:
    prefix = prefix.strip("/")
    suffix = relative_path.as_posix().lstrip("/")
    return f"{prefix}/{suffix}" if prefix else suffix


def main() -> None:
    s3 = boto3.client("s3")
    download_path = Path(
        kagglehub.dataset_download(
            DATASET_HANDLE,
            force_download=FORCE_DOWNLOAD,
        )
    )

    print(f"Downloaded {DATASET_HANDLE} to {download_path}")

    for local_file, relative_path in iter_downloaded_files(download_path):
        s3_key = build_s3_key(S3_PREFIX, relative_path)
        s3.upload_file(str(local_file), S3_BUCKET, s3_key)
        print(f"Uploaded s3://{S3_BUCKET}/{s3_key}")


if __name__ == "__main__":
    main()
