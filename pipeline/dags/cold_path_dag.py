"""
Cold Path DAG
=============
Orchestrates the cold-path pipeline on an hourly schedule.

Task flow:
  t1_consume_websocket  →  t2_update_catalog

t1: Connects to Binance WebSocket and pushes closed candles to Kinesis
    Data Stream for STREAM_DURATION_SECONDS (default 3 600 s = 1 hour).
    Kinesis Firehose (configured separately in AWS) delivers the stream
    data to S3 automatically — no code needed for that step.

t2: Ensures the Glue Data Catalog tables exist for the S3 raw data.
    This is idempotent — safe to run every slot.

AWS prerequisites (must exist before running this DAG):
  - Kinesis Data Stream named "crypto-ohlcv-1m"
  - Kinesis Firehose delivery stream pointing at crypto-ohlcv-1m → S3
  - S3 bucket matching S3_BUCKET_RAW
  - IAM permissions: kinesis:PutRecord, glue:*, s3:PutObject
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.config import settings
from pipeline.tasks.consume_websocket import consume_websocket
from pipeline.tasks.update_catalog import update_stream_catalog

default_args = {
    "owner": "bda-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cold_path",
    description="Binance WebSocket → Kinesis → S3 → Glue cold path",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["cold-path", "kinesis", "websocket"],
) as dag:

    t1_consume = PythonOperator(
        task_id="consume_websocket",
        python_callable=consume_websocket,
        op_kwargs={
            "symbols": settings.STREAM_SYMBOLS,
            "duration_seconds": settings.STREAM_DURATION_SECONDS,
        },
    )

    t2_catalog = PythonOperator(
        task_id="update_catalog",
        python_callable=update_stream_catalog,
    )

    t1_consume >> t2_catalog
