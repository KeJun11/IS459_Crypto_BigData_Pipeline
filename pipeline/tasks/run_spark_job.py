"""
Task 4 – run_spark_job
=======================
Submits a PySpark step to the existing EMR cluster and polls until it
completes (or fails).
"""

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pyarrow.parquet as pq
from pyarrow import fs

from pipeline.config import settings
from pipeline.utils.logger import get_logger
from pipeline.utils.aws_helpers import get_emr_client, get_emr_cluster_id

log = get_logger("task4.run_spark_job")

POLL_INTERVAL_SEC = 30
MAX_POLLS = 120  # ~60 min ceiling

_DAILY_OUT_BUCKET = "is459-crypto-raw-data"
_DAILY_IN_PREFIX = "bronze/binance2"
_DAILY_OUT_PREFIX = "cleaned/bq2_daily_prices_initial_full_load"
_SPARK_SCRIPT_S3 = f"s3://{_DAILY_OUT_BUCKET}/scripts/daily_ingest.py"
_SPARK_SCRIPT_LOCAL = Path(__file__).parents[1] / "spark" / "daily_ingest.py"


def _list_partition_files(yesterday: str) -> list[str]:
    """Return S3 keys of parquet files in the output partition for yesterday."""
    partition_prefix = f"{_DAILY_OUT_PREFIX}/date={yesterday}/"
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(
            Bucket=_DAILY_OUT_BUCKET, Prefix=partition_prefix
        )
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]


def _log_partition_stats(yesterday: str, parquet_keys: list[str], label: str) -> None:
    """Log row/coin counts for a list of parquet keys."""
    partition_prefix = f"{_DAILY_OUT_PREFIX}/date={yesterday}/"
    s3_fs = fs.S3FileSystem(region=settings.AWS_REGION)
    try:
        total_rows = sum(
            pq.read_metadata(f"{_DAILY_OUT_BUCKET}/{key}", filesystem=s3_fs).num_rows
            for key in parquet_keys
        )
        symbols = set()
        for key in parquet_keys:
            table = pq.read_table(
                f"{_DAILY_OUT_BUCKET}/{key}",
                columns=["symbol"],
                filesystem=s3_fs,
            )
            symbols.update(table.column("symbol").to_pylist())
        log.info(
            "%s s3://%s/%s  ➜  %d rows, %d coins (%d file(s))",
            label,
            _DAILY_OUT_BUCKET,
            partition_prefix,
            total_rows,
            len(symbols),
            len(parquet_keys),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read partition stats: %s", exc)


def run_spark_job(target_date: str | None = None) -> str:
    """
    Add a Spark step to the EMR cluster, wait for completion.

    Parameters
    ----------
    target_date : str | None
        ISO date string (e.g. '2026-03-27') to process. Defaults to yesterday.

    Returns
    -------
    str
        Final step state – 'COMPLETED', 'SKIPPED', 'FAILED', etc.
    """
    yesterday = target_date or str(
        datetime.now(timezone.utc).date() - timedelta(days=1)
    )
    log.info("═══ Task 4: run_spark_job  START ═══")

    # ── Pre-check: does the output partition already exist? ───────────────────
    existing_keys = _list_partition_files(yesterday)
    if existing_keys:
        log.warning(
            "Output partition already exists — skipping EMR step to avoid duplicates."
        )
        _log_partition_stats(yesterday, existing_keys, "Existing data:")
        log.info("═══ Task 4: run_spark_job  SKIPPED ═══")
        return "SKIPPED"

    log.info(
        "Output partition s3://%s/%s/date=%s/ is empty — proceeding.",
        _DAILY_OUT_BUCKET,
        _DAILY_OUT_PREFIX,
        yesterday,
    )

    # ── Upload latest script to S3 ────────────────────────────────────────────
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    s3.upload_file(
        str(_SPARK_SCRIPT_LOCAL), _DAILY_OUT_BUCKET, "scripts/daily_ingest.py"
    )
    log.info("Uploaded %s  ➜  %s", _SPARK_SCRIPT_LOCAL.name, _SPARK_SCRIPT_S3)

    emr = get_emr_client()
    cluster_id = get_emr_cluster_id()

    step = {
        "Name": "transform-binance-klines",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--conf",
                "spark.sql.catalogImplementation=hive",
                _SPARK_SCRIPT_S3,
                yesterday,
            ],
        },
    }

    log.info(
        "Spark input  : s3://%s/%s/%s/", _DAILY_OUT_BUCKET, _DAILY_IN_PREFIX, yesterday
    )
    log.info(
        "Spark output : s3://%s/%s/date=%s/",
        _DAILY_OUT_BUCKET,
        _DAILY_OUT_PREFIX,
        yesterday,
    )
    log.info("Submitting step to EMR cluster %s", cluster_id)
    response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    step_id = response["StepIds"][0]
    log.info("Step submitted  ➜  %s", step_id)

    for i in range(MAX_POLLS):
        desc = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = desc["Step"]["Status"]["State"]
        log.info("Poll %d  ➜  step %s  state=%s", i + 1, step_id, state)

        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            break

        time.sleep(POLL_INTERVAL_SEC)
    else:
        log.warning("Polling timed out after %d attempts", MAX_POLLS)
        state = "TIMEOUT"

    if state != "COMPLETED":
        failure = desc["Step"]["Status"].get("FailureDetails", {})
        log.error(
            "Step did not complete: %s – %s",
            state,
            failure.get("Message", "no details"),
        )
    else:
        log.info("Spark job finished successfully")
        result_keys = _list_partition_files(yesterday)
        if result_keys:
            _log_partition_stats(yesterday, result_keys, "Written:")
        else:
            log.warning(
                "No parquet files found at s3://%s/%s/date=%s/ after job completed",
                _DAILY_OUT_BUCKET,
                _DAILY_OUT_PREFIX,
                yesterday,
            )

    log.info("═══ Task 4: run_spark_job  DONE  ═══")
    return state


if __name__ == "__main__":
    final_state = run_spark_job()
    print(f"Final state: {final_state}")
