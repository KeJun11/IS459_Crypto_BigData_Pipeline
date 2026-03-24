from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from _common import (
    AWS_CONN_ID,
    BRONZE_REST_PREFIX,
    BRONZE_TO_SILVER_ENTRYPOINT,
    DEFAULT_DAG_ARGS,
    EMR_APPLICATION_ID,
    EMR_EXECUTION_ROLE_ARN,
    S3_BUCKET,
    SILVER_REST_PREFIX,
    SILVER_TO_GOLD_ENTRYPOINT,
    build_job_driver,
    build_monitoring_configuration,
    require_runtime_settings,
    with_clickhouse_arguments,
)


with DAG(
    dag_id="backfill_batch_reprocessing",
    description="On-demand Bronze-to-Silver and Silver-to-Gold reprocessing for a specific date range.",
    default_args=DEFAULT_DAG_ARGS,
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("2026-03-01", type="string"),
        "end_date": Param("2026-03-07", type="string"),
    },
    tags=["crypto", "phase7", "emr-serverless", "backfill"],
    render_template_as_native_obj=True,
) as dag:
    @task(task_id="validate_runtime_settings")
    def validate_runtime_settings() -> None:
        require_runtime_settings()

    wait_for_bronze_partition = S3KeySensor(
        task_id="wait_for_bronze_partition",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET,
        bucket_key=f"{BRONZE_REST_PREFIX}/symbol=*/year={{{{ params.start_date[:4] }}}}/month={{{{ params.start_date[5:7] }}}}/*.parquet",
        wildcard_match=True,
        timeout=60 * 30,
        poke_interval=60,
        mode="reschedule",
    )

    bronze_to_silver_job = EmrServerlessStartJobOperator(
        task_id="run_bronze_to_silver_backfill",
        aws_conn_id=AWS_CONN_ID,
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=build_job_driver(
            BRONZE_TO_SILVER_ENTRYPOINT,
            with_clickhouse_arguments([
                "--input-root",
                f"s3://{S3_BUCKET}/{BRONZE_REST_PREFIX}",
                "--silver-output",
                f"s3://{S3_BUCKET}/{SILVER_REST_PREFIX}",
                "--start-date",
                "{{ params.start_date }}",
                "--end-date",
                "{{ params.end_date }}",
            ]),
        ),
        configuration_overrides=build_monitoring_configuration(),
        wait_for_completion=True,
        waiter_delay=60,
        waiter_max_attempts=180,
        name="backfill-bronze-to-silver-{{ params.start_date | replace('-', '') }}-{{ params.end_date | replace('-', '') }}",
    )

    silver_to_gold_job = EmrServerlessStartJobOperator(
        task_id="run_silver_to_gold_backfill",
        aws_conn_id=AWS_CONN_ID,
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver=build_job_driver(
            SILVER_TO_GOLD_ENTRYPOINT,
            with_clickhouse_arguments([
                "--silver-input",
                f"s3://{S3_BUCKET}/{SILVER_REST_PREFIX}",
                "--start-date",
                "{{ params.start_date }}",
                "--end-date",
                "{{ params.end_date }}",
            ]),
        ),
        configuration_overrides=build_monitoring_configuration(),
        wait_for_completion=True,
        waiter_delay=60,
        waiter_max_attempts=180,
        name="backfill-silver-to-gold-{{ params.start_date | replace('-', '') }}-{{ params.end_date | replace('-', '') }}",
    )

    @task(task_id="summarize_backfill_run")
    def summarize_backfill_run(bronze_job_run_id: str, gold_job_run_id: str) -> dict:
        return {
            "bronze_to_silver_job_run_id": bronze_job_run_id,
            "silver_to_gold_job_run_id": gold_job_run_id,
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}",
        }

    summary = summarize_backfill_run(bronze_to_silver_job.output, silver_to_gold_job.output)

    validate_runtime_settings() >> wait_for_bronze_partition >> bronze_to_silver_job >> silver_to_gold_job >> summary
