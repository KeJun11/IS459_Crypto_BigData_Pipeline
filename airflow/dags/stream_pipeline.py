from __future__ import annotations

import shlex
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from _common import (
    AWS_REGION,
    DEFAULT_DAG_ARGS,
    TRACKED_SYMBOLS,
    get_setting,
)

# ── Stream-specific settings ──────────────────────────────────────────────────
_KINESIS_STREAM_NAME = get_setting("KINESIS_STREAM_NAME", "crypto-ohlcv-1m")
_STREAM_SPARK_CONTAINER = get_setting("STREAM_SPARK_CONTAINER", "crypto-spark-master")
_STREAM_SPARK_MASTER = get_setting("STREAM_SPARK_MASTER", "spark://spark-master:7077")
_STREAM_SPARK_PACKAGES = get_setting("STREAM_SPARK_PACKAGES", "")
_STREAM_EXTRA_JARS = get_setting("STREAM_EXTRA_JARS", "")
_STREAM_PRODUCER_SCRIPT = get_setting("STREAM_PRODUCER_SCRIPT", "src/ingestion/stream_producer.py")
_STREAM_SCHEMA_PATH = get_setting("STREAM_SCHEMA_PATH", "schemas/kline_1m.schema.json")
_STREAM_MAX_RUNTIME_SECONDS = int(get_setting("STREAM_MAX_RUNTIME_SECONDS", "3600"))
_STREAM_AWAIT_TERMINATION_SECONDS = int(get_setting("STREAM_AWAIT_TERMINATION_SECONDS", "3600"))
_PROJECT_ROOT = get_setting("AIRFLOW_PROJECT_ROOT", "/opt/airflow/project")


def _build_producer_command() -> str:
    symbols_str = " ".join(shlex.quote(s) for s in TRACKED_SYMBOLS)
    cmd = (
        f"python {shlex.quote(_STREAM_PRODUCER_SCRIPT)}"
        f" --stream-name {shlex.quote(_KINESIS_STREAM_NAME)}"
        f" --region {shlex.quote(AWS_REGION)}"
        f" --symbols {symbols_str}"
        f" --schema-path {shlex.quote(_STREAM_SCHEMA_PATH)}"
        f" --max-runtime-seconds {_STREAM_MAX_RUNTIME_SECONDS}"
    )
    return f"cd {shlex.quote(_PROJECT_ROOT)} && {cmd}"


def _build_consumer_command() -> str:
    cmd = (
        f"python scripts/run_stream_to_silver.py"
        f" --docker-container {shlex.quote(_STREAM_SPARK_CONTAINER)}"
        f" --master {shlex.quote(_STREAM_SPARK_MASTER)}"
    )
    if _STREAM_SPARK_PACKAGES:
        cmd += f" --packages {shlex.quote(_STREAM_SPARK_PACKAGES)}"
    if _STREAM_EXTRA_JARS:
        cmd += f" --jars {shlex.quote(_STREAM_EXTRA_JARS)}"
    # --await-termination-seconds is an unknown arg to run_stream_to_silver.py,
    # so parse_known_args forwards it directly to stream_to_silver.py via spark-submit.
    cmd += f" --await-termination-seconds {_STREAM_AWAIT_TERMINATION_SECONDS}"
    return f"cd {shlex.quote(_PROJECT_ROOT)} && {cmd}"


with DAG(
    dag_id="stream_pipeline",
    description="Binance WebSocket → Kinesis producer running in parallel with Kinesis → Silver Spark consumer.",
    default_args=DEFAULT_DAG_ARGS,
    start_date=datetime(2026, 4, 5),
    schedule="@hourly",
    catchup=False,
    tags=["crypto", "phase7", "stream", "kinesis"],
) as dag:

    @task(task_id="validate_runtime_settings")
    def validate_runtime_settings() -> None:
        missing = [
            name
            for name, value in {
                "KINESIS_STREAM_NAME": _KINESIS_STREAM_NAME,
                "STREAM_SPARK_CONTAINER": _STREAM_SPARK_CONTAINER,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"Missing stream runtime settings: {', '.join(missing)}")

    stream_producer = BashOperator(
        task_id="stream_producer",
        bash_command=_build_producer_command(),
    )

    stream_to_silver = BashOperator(
        task_id="stream_to_silver",
        bash_command=_build_consumer_command(),
    )

    @task(task_id="summarize_stream_run")
    def summarize_stream_run() -> dict:
        context = get_current_context()
        return {
            "data_interval_start": context["data_interval_start"].isoformat(),
            "data_interval_end": context["data_interval_end"].isoformat(),
            "stream_name": _KINESIS_STREAM_NAME,
            "symbols": TRACKED_SYMBOLS,
            "producer_runtime_seconds": _STREAM_MAX_RUNTIME_SECONDS,
            "consumer_termination_seconds": _STREAM_AWAIT_TERMINATION_SECONDS,
        }

    settings_check = validate_runtime_settings()
    summary = summarize_stream_run()

    settings_check >> [stream_producer, stream_to_silver]
    [stream_producer, stream_to_silver] >> summary
