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
_GLUE_DATABASE = get_setting("GLUE_DATABASE", "crypto_pipeline_db")
_STREAM_SILVER_OUTPUT = get_setting("STREAM_SILVER_OUTPUT", "s3a://is459-crypto-datalake/silver/stream")
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
    max_active_runs=1,
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

    @task(task_id="update_glue_catalog")
    def update_glue_catalog() -> None:
        import boto3

        # Glue requires s3:// — strip s3a:// if present
        s3_location = _STREAM_SILVER_OUTPUT.replace("s3a://", "s3://").rstrip("/")

        silver_table = {
            "Name": "silver_stream_ohlcv_1m",
            "Description": "Cleaned 1-minute OHLCV stream data written by Spark Structured Streaming (Parquet)",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "symbol",                      "Type": "string"},
                    {"Name": "open",                        "Type": "double"},
                    {"Name": "high",                        "Type": "double"},
                    {"Name": "low",                         "Type": "double"},
                    {"Name": "close",                       "Type": "double"},
                    {"Name": "volume",                      "Type": "double"},
                    {"Name": "open_time",                   "Type": "bigint"},
                    {"Name": "close_time",                  "Type": "bigint"},
                    {"Name": "is_closed",                   "Type": "boolean"},
                    {"Name": "source",                      "Type": "string"},
                    {"Name": "processed_at",                "Type": "timestamp"},
                    {"Name": "open_time_ts",                "Type": "timestamp"},
                    {"Name": "event_time_ts",               "Type": "timestamp"},
                    {"Name": "kinesis_arrival_timestamp",   "Type": "timestamp"},
                ],
                "Location": s3_location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            },
            "PartitionKeys": [
                {"Name": "symbol", "Type": "string"},
                {"Name": "year",   "Type": "int"},
                {"Name": "month",  "Type": "int"},
            ],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet"},
        }

        glue = boto3.client("glue", region_name=AWS_REGION)

        try:
            glue.get_database(Name=_GLUE_DATABASE)
        except glue.exceptions.EntityNotFoundException:
            glue.create_database(
                DatabaseInput={
                    "Name": _GLUE_DATABASE,
                    "Description": "Crypto data lake – Binance klines",
                }
            )

        try:
            glue.get_table(DatabaseName=_GLUE_DATABASE, Name=silver_table["Name"])
            glue.update_table(DatabaseName=_GLUE_DATABASE, TableInput=silver_table)
        except glue.exceptions.EntityNotFoundException:
            glue.create_table(DatabaseName=_GLUE_DATABASE, TableInput=silver_table)

    settings_check = validate_runtime_settings()
    catalog_update = update_glue_catalog()
    summary = summarize_stream_run()

    settings_check >> [stream_producer, stream_to_silver]
    [stream_producer, stream_to_silver] >> catalog_update >> summary
