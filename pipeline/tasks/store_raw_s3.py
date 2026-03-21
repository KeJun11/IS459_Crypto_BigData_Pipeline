"""
Task 2 – store_raw_to_s3
=========================
Takes the dict returned by Task 1 and uploads each symbol's kline data
as a JSON file to S3 under:

    s3://<bucket>/raw/binance/<symbol>/<YYYY-MM-DD>.json
"""

import json
import datetime as dt
from typing import Any

from pipeline.utils.aws_helpers import get_s3_client
from pipeline.config import settings
from pipeline.utils.logger import get_logger

log = get_logger("task2.store_raw_to_s3")


def store_raw_to_s3(data: dict[str, list[dict[str, Any]]]) -> list[str]:
    """
    Upload every symbol's klines to S3 as JSON.

    Parameters
    ----------
    data : dict
        Output of task1.pull_api_data(), shaped as
        { symbol: [ {kline_record}, … ] }.

    Returns
    -------
    list[str]
        S3 keys that were written.
    """
    log.info("═══ Task 2: store_raw_to_s3  START ═══")
    s3 = get_s3_client()
    today = dt.date.today().isoformat()
    uploaded_keys: list[str] = []

    for symbol, records in data.items():
        if not records:
            log.warning("Skipping %s — no data", symbol)
            continue

        s3_key = f"{settings.S3_RAW_PREFIX}binance/{symbol}/{today}.json"
        body = json.dumps(records, default=str, ensure_ascii=False)

        log.info("Uploading  ➜  s3://%s/%s  (%d records, %d bytes)",
                 settings.S3_BUCKET_RAW, s3_key, len(records), len(body))

        s3.put_object(
            Bucket=settings.S3_BUCKET_RAW,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
        uploaded_keys.append(s3_key)

    log.info("═══ Task 2: store_raw_to_s3  DONE  ═══  (%d files)", len(uploaded_keys))
    return uploaded_keys


if __name__ == "__main__":
    from pipeline.tasks.pull_api import pull_api_data

    data = pull_api_data()
    keys = store_raw_to_s3(data)
    for k in keys:
        print(f"  ✓ {k}")