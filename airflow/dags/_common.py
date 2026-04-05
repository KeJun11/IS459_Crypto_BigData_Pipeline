from __future__ import annotations

import os
import urllib.parse
import urllib.request
from datetime import timedelta

from airflow.models import Variable


def get_setting(name: str, default: str = "") -> str:
    env_value = os.getenv(name)
    if env_value is not None and env_value != "":
        return env_value
    return Variable.get(name, default_var=default)


def get_first_setting(names: list[str], default: str = "") -> str:
    for name in names:
        value = get_setting(name, "")
        if value:
            return value
    return default


def build_clickhouse_http_url(host_names: list[str], port_names: list[str], default_port: str = "8123") -> str:
    host = get_first_setting(host_names, "")
    if not host:
        return ""
    port = get_first_setting(port_names, default_port)
    return f"http://{host}:{port}"


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
CLICKHOUSE_URL = get_first_setting(
    ["AIRFLOW_CLICKHOUSE_URL"],
    build_clickhouse_http_url(
        ["AIRFLOW_CLICKHOUSE_HOST", "SHARED_CLICKHOUSE_HOST"],
        ["AIRFLOW_CLICKHOUSE_PORT", "SHARED_CLICKHOUSE_PORT"],
    ),
)
CLICKHOUSE_VALIDATION_URL = get_first_setting(
    ["AIRFLOW_CLICKHOUSE_VALIDATION_URL"],
    CLICKHOUSE_URL,
)
CLICKHOUSE_DATABASE = get_first_setting(
    ["AIRFLOW_CLICKHOUSE_DATABASE", "SHARED_CLICKHOUSE_DATABASE"],
    "crypto",
)
TRACKED_SYMBOLS = [value.strip() for value in get_setting("AIRFLOW_TRACKED_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if value.strip()]
SKIP_BATCH_CLICKHOUSE = get_setting("AIRFLOW_BATCH_SKIP_CLICKHOUSE", "false").lower() in {"1", "true", "yes"}
BASE_SPARK_SUBMIT_PARAMETERS = get_setting(
    "AIRFLOW_EMR_SPARK_SUBMIT_PARAMETERS",
    "--conf spark.dynamicAllocation.enabled=false --conf spark.executor.instances=1 --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.driver.cores=1 --conf spark.driver.memory=1g",
)
DEFAULT_RETRIES = int(get_setting("AIRFLOW_DEFAULT_RETRIES", "2"))
DEFAULT_RETRY_DELAY_MINUTES = int(get_setting("AIRFLOW_DEFAULT_RETRY_DELAY_MINUTES", "5"))
MAX_RETRY_DELAY_MINUTES = int(get_setting("AIRFLOW_MAX_RETRY_DELAY_MINUTES", "30"))

DEFAULT_DAG_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=DEFAULT_RETRY_DELAY_MINUTES),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=MAX_RETRY_DELAY_MINUTES),
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


def build_clickhouse_interval_filter(column: str, start_ts: str, end_ts: str) -> str:
    return (
        f"{column} >= toDateTime64('{start_ts}', 3, 'UTC') "
        f"AND {column} < toDateTime64('{end_ts}', 3, 'UTC')"
    )


def clickhouse_scalar(query: str, url: str | None = None) -> str:
    target_url = url or CLICKHOUSE_VALIDATION_URL
    if not target_url:
        raise ValueError("Missing AIRFLOW_CLICKHOUSE_VALIDATION_URL or AIRFLOW_CLICKHOUSE_URL for validation queries.")
    request = urllib.request.Request(
        url=f"{target_url}/?query={urllib.parse.quote(query)}",
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8").strip()
