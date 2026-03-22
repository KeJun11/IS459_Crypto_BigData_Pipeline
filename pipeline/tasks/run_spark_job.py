"""
Task 4 – run_spark_job
=======================
Submits a PySpark step to the existing EMR cluster and polls until it
completes (or fails).

The PySpark script (spark/transform.py) should be uploaded to S3
at settings.SPARK_SCRIPT_S3_PATH before running this task.
"""

import time

from pipeline.config import settings
from pipeline.utils.logger import get_logger
from pipeline.utils.aws_helpers import get_emr_client

log = get_logger("task4.run_spark_job")

POLL_INTERVAL_SEC = 30
MAX_POLLS = 120  # ~60 min ceiling


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

    step = {
        "Name": "transform-binance-klines",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.sql.catalogImplementation=hive",
                settings.SPARK_SCRIPT_S3_PATH,
                "--source-db", settings.GLUE_DATABASE,
                "--output-bucket", settings.S3_BUCKET_RAW,
            ],
        },
    }

    log.info("Submitting step to EMR cluster %s", settings.EMR_CLUSTER_ID)
    response = emr.add_job_flow_steps(
        JobFlowId=settings.EMR_CLUSTER_ID,
        Steps=[step],
    )
    step_id = response["StepIds"][0]
    log.info("Step submitted  ➜  %s", step_id)

    for i in range(MAX_POLLS):
        desc = emr.describe_step(
            ClusterId=settings.EMR_CLUSTER_ID,
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
        log.error("Step did not complete: %s – %s",
                  state, failure.get("Message", "no details"))
    else:
        log.info("Spark job finished successfully")

    log.info("═══ Task 4: run_spark_job  DONE  ═══")
    return state


if __name__ == "__main__":
    final_state = run_spark_job()
    print(f"Final state: {final_state}")