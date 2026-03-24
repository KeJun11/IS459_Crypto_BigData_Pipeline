from __future__ import annotations

import os
from datetime import timedelta

from airflow.models import Variable


def get_setting(name: str, default: str = "") -> str:
    env_value = os.getenv(name)
    if env_value is not None and env_value != "":
        return env_value
    return Variable.get(name, default_var=default)


AWS_CONN_ID = get_setting("AIRFLOW_AWS_CONN_ID", "aws_default")
AWS_REGION = get_setting("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET = get_setting("BATCH_S3_BUCKET", "")
BRONZE_REST_PREFIX = get_setting("AIRFLOW_BRONZE_REST_PREFIX", get_setting("BATCH_S3_PREFIX", "bronze/rest/binance-klines"))
SILVER_REST_PREFIX = get_setting("AIRFLOW_SILVER_REST_PREFIX", "silver/rest")
EMR_APPLICATION_ID = get_setting("AIRFLOW_EMR_SERVERLESS_APPLICATION_ID", "")
EMR_EXECUTION_ROLE_ARN = get_setting("AIRFLOW_EMR_SERVERLESS_EXECUTION_ROLE_ARN", "")
EMR_LOG_URI = get_setting("AIRFLOW_EMR_SERVERLESS_LOG_URI", "")
BRONZE_TO_SILVER_ENTRYPOINT = get_setting("AIRFLOW_BRONZE_TO_SILVER_ENTRYPOINT", "s3://replace-me/jobs/bronze_to_silver_batch.py")
SILVER_TO_GOLD_ENTRYPOINT = get_setting("AIRFLOW_SILVER_TO_GOLD_ENTRYPOINT", "s3://replace-me/jobs/silver_to_gold_batch.py")
EMR_PY_FILES = [value.strip() for value in get_setting("AIRFLOW_EMR_PY_FILES", "").split(",") if value.strip()]
CLICKHOUSE_URL = get_setting("AIRFLOW_CLICKHOUSE_URL", "")
CLICKHOUSE_DATABASE = get_setting("AIRFLOW_CLICKHOUSE_DATABASE", "crypto")
SKIP_BATCH_CLICKHOUSE = get_setting("AIRFLOW_BATCH_SKIP_CLICKHOUSE", "false").lower() in {"1", "true", "yes"}
BASE_SPARK_SUBMIT_PARAMETERS = get_setting(
    "AIRFLOW_EMR_SPARK_SUBMIT_PARAMETERS",
    "--conf spark.executor.memory=2g --conf spark.driver.memory=1g --conf spark.executor.cores=2",
)
DEFAULT_RETRIES = int(get_setting("AIRFLOW_DEFAULT_RETRIES", "2"))
DEFAULT_RETRY_DELAY_MINUTES = int(get_setting("AIRFLOW_DEFAULT_RETRY_DELAY_MINUTES", "5"))

DEFAULT_DAG_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=DEFAULT_RETRY_DELAY_MINUTES),
}


def build_job_driver(entry_point: str, entry_point_arguments: list[str]) -> dict:
    spark_submit_parameters = BASE_SPARK_SUBMIT_PARAMETERS.strip()
    if EMR_PY_FILES:
        py_files_arg = ",".join(EMR_PY_FILES)
        spark_submit_parameters = f"{spark_submit_parameters} --py-files {py_files_arg}".strip()
    return {
        "sparkSubmit": {
            "entryPoint": entry_point,
            "entryPointArguments": entry_point_arguments,
            "sparkSubmitParameters": spark_submit_parameters,
        }
    }


def build_monitoring_configuration() -> dict:
    if not EMR_LOG_URI:
        return {}
    return {
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": EMR_LOG_URI,
            }
        }
    }


def require_runtime_settings() -> None:
    missing = [
        name
        for name, value in {
            "BATCH_S3_BUCKET": S3_BUCKET,
            "AIRFLOW_EMR_SERVERLESS_APPLICATION_ID": EMR_APPLICATION_ID,
            "AIRFLOW_EMR_SERVERLESS_EXECUTION_ROLE_ARN": EMR_EXECUTION_ROLE_ARN,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError(f"Missing Airflow runtime settings: {', '.join(missing)}")


def with_clickhouse_arguments(arguments: list[str]) -> list[str]:
    resolved_arguments = list(arguments)
    if CLICKHOUSE_URL:
        resolved_arguments.extend(["--clickhouse-url", CLICKHOUSE_URL])
        if CLICKHOUSE_DATABASE:
            resolved_arguments.extend(["--clickhouse-database", CLICKHOUSE_DATABASE])
    elif SKIP_BATCH_CLICKHOUSE:
        resolved_arguments.append("--skip-clickhouse")
    else:
        resolved_arguments.append("--skip-clickhouse")
    return resolved_arguments
