from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.jobs import bronze_to_silver_batch as bronze_job  # noqa: E402
from src.jobs import silver_to_gold_batch as gold_job  # noqa: E402


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("phase6_batch_workflow_container_smoke_test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def bronze_schema() -> StructType:
    return StructType(
        [
            StructField("symbol", StringType(), nullable=False),
            StructField("open", DoubleType(), nullable=False),
            StructField("high", DoubleType(), nullable=False),
            StructField("low", DoubleType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
            StructField("volume", DoubleType(), nullable=False),
            StructField("open_time", LongType(), nullable=False),
            StructField("close_time", LongType(), nullable=False),
            StructField("is_closed", BooleanType(), nullable=False),
            StructField("source", StringType(), nullable=False),
            StructField("ingested_at", LongType(), nullable=False),
        ]
    )


def build_sample_rows() -> list[tuple]:
    return [
        ("BTCUSDT", 100.0, 102.0, 99.0, 101.0, 10.0, 1772323200000, 1772323259999, True, "binance_rest", 1772323300000),
        ("BTCUSDT", 101.0, 103.0, 100.0, 102.0, 11.0, 1772323260000, 1772323319999, True, "binance_rest", 1772323360000),
        ("BTCUSDT", 101.0, 103.5, 100.0, 102.5, 11.5, 1772323260000, 1772323319999, True, "binance_rest", 1772323420000),
        ("BTCUSDT", 102.5, 104.0, 102.0, 103.0, 12.0, 1772326800000, 1772326859999, True, "binance_rest", 1772326900000),
        ("BTCUSDT", 103.0, 105.0, 102.5, 104.0, 13.0, 1772326860000, 1772326919999, True, "binance_rest", 1772326960000),
        ("ETHUSDT", 200.0, 201.0, 199.0, 200.5, 5.0, 1772323200000, 1772323259999, True, "binance_rest", 1772323300000),
        ("BTCUSDT", 110.0, 105.0, 111.0, 109.0, 8.0, 1772330400000, 1772330459999, True, "binance_rest", 1772330500000),
    ]


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    output_root = Path(tempfile.mkdtemp(prefix="phase6_batch_test_"))
    bronze_output = output_root / "bronze"
    silver_output = output_root / "silver"
    hourly_output = output_root / "gold_hourly"
    daily_output = output_root / "gold_daily"

    try:
        bronze_df = spark.createDataFrame(build_sample_rows(), schema=bronze_schema())
        bronze_df.write.mode("overwrite").parquet(str(bronze_output))

        typed_bronze_df = bronze_job.build_typed_bronze_df(spark.read.parquet(str(bronze_output)))
        invalid_bronze_df = bronze_job.build_invalid_bronze_df(typed_bronze_df)
        valid_bronze_df = bronze_job.build_valid_bronze_df(typed_bronze_df)
        cleaned_silver_df = bronze_job.build_cleaned_silver_df(valid_bronze_df)

        assert invalid_bronze_df.count() == 1, "expected one invalid Bronze row"
        assert valid_bronze_df.count() == 6, "expected six valid Bronze rows before deduplication"
        assert cleaned_silver_df.count() == 5, "expected five cleaned Silver rows after deduplication"

        bronze_job.write_silver_parquet(cleaned_silver_df, str(silver_output))
        silver_written_df = spark.read.parquet(str(silver_output))
        assert silver_written_df.count() == 5, "expected five rows in Silver parquet output"

        raw_clickhouse_rows = bronze_job.build_clickhouse_raw_rows(cleaned_silver_df)
        assert len(raw_clickhouse_rows) == 5, "expected five ClickHouse raw rows"
        deduped_btc_row = next(
            row for row in raw_clickhouse_rows if row["symbol"] == "BTCUSDT" and row["open_time"] == "2026-03-01 00:01:00.000"
        )
        assert abs(deduped_btc_row["close"] - 102.5) < 1e-9, "expected latest Bronze row to win during deduplication"

        silver_df = gold_job.load_silver_df(spark, str(silver_output))
        hourly_df = gold_job.build_hourly_aggregate_df(silver_df)
        daily_df = gold_job.build_daily_aggregate_df(silver_df)

        gold_job.write_optional_snapshot(hourly_df, str(hourly_output))
        gold_job.write_optional_snapshot(daily_df, str(daily_output))

        assert spark.read.parquet(str(hourly_output)).count() == 3, "expected three hourly aggregate rows"
        assert spark.read.parquet(str(daily_output)).count() == 2, "expected two daily aggregate rows"

        hourly_rows = gold_job.build_clickhouse_aggregate_rows(hourly_df)
        daily_rows = gold_job.build_clickhouse_aggregate_rows(daily_df)
        assert len(hourly_rows) == 3, "expected three hourly ClickHouse rows"
        assert len(daily_rows) == 2, "expected two daily ClickHouse rows"

        btc_hourly_row = next(
            row for row in hourly_rows if row["symbol"] == "BTCUSDT" and row["bucket_start"] == "2026-03-01 00:00:00.000"
        )
        assert abs(btc_hourly_row["open"] - 100.0) < 1e-9, "incorrect hourly open aggregation"
        assert abs(btc_hourly_row["close"] - 102.5) < 1e-9, "incorrect hourly close aggregation"
        assert abs(btc_hourly_row["volume"] - 21.5) < 1e-9, "incorrect hourly volume aggregation"

        btc_daily_row = next(row for row in daily_rows if row["symbol"] == "BTCUSDT")
        assert abs(btc_daily_row["high"] - 105.0) < 1e-9, "incorrect daily high aggregation"

        print("Verified Bronze validation and Silver deduplication")
        print("Verified Silver parquet output and raw ClickHouse row preparation")
        print("Verified hourly and daily Gold aggregate generation")
        print("Phase 6 batch workflow smoke test passed")
    finally:
        spark.stop()
        shutil.rmtree(output_root, ignore_errors=True)


if __name__ == "__main__":
    main()
