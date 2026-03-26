from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from _common import (
    AWS_CONN_ID,
    BRONZE_REST_PREFIX,
    BRONZE_TO_SILVER_ENTRYPOINT,
    CLICKHOUSE_DATABASE,
    DEFAULT_DAG_ARGS,
    EMR_APPLICATION_ID,
    EMR_EXECUTION_ROLE_ARN,
    S3_BUCKET,
    SILVER_REST_PREFIX,
    SILVER_TO_GOLD_ENTRYPOINT,
    build_job_driver,
    build_clickhouse_interval_filter,
    build_monitoring_configuration,
    clickhouse_scalar,
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

    @task(task_id="validate_backfill_counts")
    def validate_backfill_counts(bronze_job_run_id: str, gold_job_run_id: str) -> dict:
        context = get_current_context()
        start_date = context["params"]["start_date"]
        end_date = context["params"]["end_date"]
        interval_start = f"{start_date} 00:00:00.000"
        interval_end = f"{end_date} 23:59:59.999"

        raw_count = int(
            clickhouse_scalar(
                f"SELECT count() FROM {CLICKHOUSE_DATABASE}.raw_ohlcv_1m "
                f"WHERE {build_clickhouse_interval_filter('open_time', interval_start, interval_end)}"
            )
        )
        hourly_count = int(
            clickhouse_scalar(
                f"SELECT count() FROM {CLICKHOUSE_DATABASE}.agg_ohlcv_1h "
                f"WHERE {build_clickhouse_interval_filter('bucket_start', interval_start, interval_end)}"
            )
        )
        daily_count = int(
            clickhouse_scalar(
                f"SELECT count() FROM {CLICKHOUSE_DATABASE}.agg_ohlcv_1d "
                f"WHERE {build_clickhouse_interval_filter('bucket_start', interval_start, interval_end)}"
            )
        )

        if raw_count <= 0:
            raise ValueError(f"No Silver rows found for backfill window {start_date} to {end_date}.")
        if hourly_count <= 0 or daily_count <= 0:
            raise ValueError(
                f"Backfill aggregate validation failed for {start_date} to {end_date}: "
                f"hourly_count={hourly_count}, daily_count={daily_count}"
            )

        return {
            "bronze_to_silver_job_run_id": bronze_job_run_id,
            "silver_to_gold_job_run_id": gold_job_run_id,
            "raw_ohlcv_1m_count": raw_count,
            "agg_ohlcv_1h_count": hourly_count,
            "agg_ohlcv_1d_count": daily_count,
        }

    wait_for_bronze_partition = S3KeySensor(
        task_id="wait_for_bronze_partition",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET,
        bucket_key=f"{BRONZE_REST_PREFIX}/symbol=*/year={{{{ params.start_date[:4] | int }}}}/month={{{{ params.start_date[5:7] | int }}}}/*.parquet",
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
    def summarize_backfill_run(validation_result: dict) -> dict:
        context = get_current_context()
        validation_result["start_date"] = context["params"]["start_date"]
        validation_result["end_date"] = context["params"]["end_date"]
        return validation_result

    validated_counts = validate_backfill_counts(bronze_to_silver_job.output, silver_to_gold_job.output)
    summary = summarize_backfill_run(validated_counts)

    validate_runtime_settings() >> wait_for_bronze_partition >> bronze_to_silver_job >> silver_to_gold_job >> validated_counts >> summary
