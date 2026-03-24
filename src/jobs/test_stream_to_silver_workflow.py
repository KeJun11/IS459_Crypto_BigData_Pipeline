from __future__ import annotations

import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.jobs import stream_to_silver as stream_job  # noqa: E402


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("stream_to_silver_container_smoke_test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def build_sample_rows() -> list[tuple[str, datetime]]:
    return [
        (
            json.dumps(
                {
                    "symbol": "BTCUSDT",
                    "open": 66973.26,
                    "high": 67029.25,
                    "low": 66967.18,
                    "close": 66998.58,
                    "volume": 9.19306,
                    "open_time": 1772323200000,
                    "close_time": 1772323259999,
                    "is_closed": True,
                }
            ),
            datetime(2026, 3, 1, 0, 0, 5, tzinfo=UTC),
        ),
        (
            json.dumps(
                {
                    "symbol": "BTCUSDT",
                    "open": 66973.26,
                    "high": 67029.25,
                    "low": 66967.18,
                    "close": 66998.58,
                    "volume": 9.19306,
                    "open_time": 1772323200000,
                    "close_time": 1772323259999,
                    "is_closed": True,
                }
            ),
            datetime(2026, 3, 1, 0, 0, 6, tzinfo=UTC),
        ),
        (
            json.dumps(
                {
                    "symbol": "ETHUSDT",
                    "open": 3440.11,
                    "high": 3445.21,
                    "low": 3438.9,
                    "close": 3444.5,
                    "volume": 22.8,
                    "open_time": 1772323200000,
                    "close_time": 1772323259999,
                    "is_closed": True,
                }
            ),
            datetime(2026, 3, 1, 0, 0, 7, tzinfo=UTC),
        ),
        (
            "{\"symbol\":\"BROKEN\",\"open\":\"bad-json\"",
            datetime(2026, 3, 1, 0, 0, 8, tzinfo=UTC),
        ),
    ]


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("data", StringType(), nullable=False),
            StructField("approximateArrivalTimestamp", TimestampType(), nullable=False),
        ]
    )
    raw_df = spark.createDataFrame(build_sample_rows(), schema=schema)

    parsed_df = stream_job.build_parsed_stream_df(raw_df)
    invalid_df = stream_job.build_invalid_stream_df(parsed_df)
    valid_df = stream_job.build_valid_stream_df(parsed_df)
    cleaned_df = stream_job.build_cleaned_stream_df(valid_df, streaming_mode=False)

    assert invalid_df.count() == 1, "expected one invalid record"
    assert valid_df.count() == 3, "expected three valid records before dedup"
    assert cleaned_df.count() == 2, "expected two cleaned records after dedup"

    output_root = Path(tempfile.mkdtemp(prefix="stream_to_silver_container_test_"))
    silver_output = output_root / "silver"
    captured_rows: list[dict] = []
    captured_metrics: list[dict] = []

    original_insert_df = stream_job.insert_dataframe_into_clickhouse
    original_insert_metrics = stream_job.insert_metric_rows

    def fake_insert_dataframe(batch_df, clickhouse_url, database, table) -> None:
        captured_rows.extend(
            row.asDict() for row in batch_df.select(
                "symbol",
                "open_time_str",
                "close_time_str",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "is_closed_int",
                "source",
            ).collect()
        )

    def fake_insert_metrics(clickhouse_url, database, table, rows) -> None:
        captured_metrics.extend(rows)

    stream_job.insert_dataframe_into_clickhouse = fake_insert_dataframe
    stream_job.insert_metric_rows = fake_insert_metrics

    try:
        stream_job.write_cleaned_batch(
            batch_df=cleaned_df,
            batch_id=1,
            silver_output=str(silver_output),
            clickhouse_url="http://unused",
            database="crypto",
            table="raw_ohlcv_1m",
            metrics_table="pipeline_metrics",
            job_name="stream_to_silver_test",
            run_id="test-run",
        )
        stream_job.write_invalid_metrics_batch(
            batch_df=invalid_df,
            batch_id=1,
            clickhouse_url="http://unused",
            database="crypto",
            metrics_table="pipeline_metrics",
            job_name="stream_to_silver_test",
            run_id="test-run",
        )
    finally:
        stream_job.insert_dataframe_into_clickhouse = original_insert_df
        stream_job.insert_metric_rows = original_insert_metrics

    assert spark.read.parquet(str(silver_output)).count() == 2, "expected two silver parquet rows"
    assert len(captured_rows) == 2, "expected two clickhouse-ready rows"
    metric_names = {row["metric_name"] for row in captured_metrics}
    assert metric_names == {"silver_row_count", "ingestion_lag_seconds", "bad_record_count"}

    print("Verified invalid-row detection, valid-row parsing, and deduplication")
    print("Verified Silver parquet batch write")
    print("Verified ClickHouse payload preparation and pipeline metric generation")
    print("stream_to_silver container smoke test passed")

    spark.stop()
    shutil.rmtree(output_root)


if __name__ == "__main__":
    main()
