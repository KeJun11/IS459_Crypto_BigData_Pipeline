"""
run_pipeline.py
===============
Runs Tasks 1 → 4 sequentially.

Usage:
    python run_pipeline.py              # run all tasks
    python run_pipeline.py --dry-run    # task 1 only (Binance pull), skip AWS calls
"""

import argparse
import json
import sys

from pipeline.tasks.pull_api import pull_api_data
from pipeline.tasks.store_raw_s3 import store_raw_to_s3
from pipeline.tasks.update_catalog import update_catalog
from pipeline.tasks.run_spark_job import run_spark_job
from pipeline.utils.logger import get_logger

log = get_logger("pipeline")


def main():
    parser = argparse.ArgumentParser(description="Crypto data pipeline (Tasks 1-4)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only run Task 1 (Binance API pull) and print results. "
             "No AWS calls.",
    )
    args = parser.parse_args()

    data = pull_api_data()

    if args.dry_run:
        log.info("DRY RUN — printing summary and exiting")
        for symbol, records in data.items():
            if records:
                first_ts = records[0].get("timestamp", "?")
                last_ts = records[-1].get("timestamp", "?")
                print(f"  {symbol}: {len(records)} klines  [{first_ts} → {last_ts}]")
            else:
                print(f"  {symbol}: EMPTY")
        return

    uploaded_keys = store_raw_to_s3(data)

    update_catalog()

    state = run_spark_job()

    if state != "COMPLETED":
        log.error("Pipeline finished with Spark state: %s", state)
        sys.exit(1)

    log.info("Pipeline finished successfully. %d raw files ingested.", len(uploaded_keys))


if __name__ == "__main__":
    main()