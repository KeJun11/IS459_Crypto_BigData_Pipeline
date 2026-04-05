import boto3
import pyarrow.parquet as pq
from pyarrow import fs

BUCKET = "is459-crypto-raw-data"
PREFIX = "cleaned/bq2_daily_prices_initial_full_load/date=2017-09-06/"
REGION = "us-east-1"

s3 = boto3.client("s3", region_name=REGION)
s3_fs = fs.S3FileSystem(region=REGION)

paginator = s3.get_paginator("list_objects_v2")
parquet_keys = [
    obj["Key"]
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX)
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
]

print(f"Files found: {len(parquet_keys)}")

total_rows = sum(
    pq.read_metadata(f"{BUCKET}/{key}", filesystem=s3_fs).num_rows
    for key in parquet_keys
)

print(f"Total rows: {total_rows}")
