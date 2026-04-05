"""
Task 4 – run_spark_job
=======================
Submits a PySpark step to the existing EMR cluster and polls until it
completes (or fails).

The PySpark script (spark/transform.py) should be uploaded to S3
at settings.SPARK_SCRIPT_S3_PATH before running this task.
"""

import time
from datetime import date, timedelta

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
_DAILY_OUT_PREFIX = "cleaned/bq2_daily_prices"


def _log_rows_ingested() -> None:
    yesterday = str(date.today() - timedelta(days=1))
    partition_prefix = f"{_DAILY_OUT_PREFIX}/date={yesterday}/"
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    s3_fs = fs.S3FileSystem(region=settings.AWS_REGION)
    try:
        paginator = s3.get_paginator("list_objects_v2")
        parquet_keys = [
            obj["Key"]
            for page in paginator.paginate(
                Bucket=_DAILY_OUT_BUCKET, Prefix=partition_prefix
            )
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        if not parquet_keys:
            log.warning(
                "No parquet files found at s3://%s/%s",
                _DAILY_OUT_BUCKET,
                partition_prefix,
            )
            return
        total_rows = sum(
            pq.read_metadata(f"{_DAILY_OUT_BUCKET}/{key}", filesystem=s3_fs).num_rows
            for key in parquet_keys
        )
        log.info(
            "Rows ingested into s3://%s/%s  ➜  %d rows (%d file(s))",
            _DAILY_OUT_BUCKET,
            partition_prefix,
            total_rows,
            len(parquet_keys),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not count ingested rows: %s", exc)


def run_spark_job() -> str:
    """
    Add a Spark step to the EMR cluster, wait for completion.

    Returns
    -------
    str
        Final step state – 'COMPLETED', 'FAILED', etc.
    """
    log.info("═══ Task 4: run_spark_job  START ═══")
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
                settings.SPARK_SCRIPT_S3_PATH,
            ],
        },
    }

    log.info("Submitting step to EMR cluster %s", cluster_id)
    response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[step],
    )
    step_id = response["StepIds"][0]
    log.info("Step submitted  ➜  %s", step_id)

    for i in range(MAX_POLLS):
        desc = emr.describe_step(
            ClusterId=cluster_id,
            StepId=step_id,
        )
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
        _log_rows_ingested()

    log.info("═══ Task 4: run_spark_job  DONE  ═══")
    return state


if __name__ == "__main__":
    final_state = run_spark_job()
    print(f"Final state: {final_state}")
