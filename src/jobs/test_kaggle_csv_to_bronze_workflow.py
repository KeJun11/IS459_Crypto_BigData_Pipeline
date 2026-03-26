from __future__ import annotations

import csv
import shutil
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.jobs import kaggle_csv_to_bronze as conversion_job  # noqa: E402


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("kaggle_csv_to_bronze_container_smoke_test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def write_sample_csv(csv_path: Path) -> None:
    rows = [
        {
            "timestamp": "2017-08-17 04:00:00",
            "open": "4261.48",
            "high": "4261.48",
            "low": "4261.48",
            "close": "4261.48",
            "volume": "1.775183",
            "close_time": "2017-08-17 04:00:59.999",
            "quote_asset_volume": "7564.90685084",
            "number_of_trades": "3",
            "taker_buy_base_asset_volume": "0.075183",
            "taker_buy_quote_asset_volume": "320.39085084",
            "ignore": "0",
        },
        {
            "timestamp": "2017-08-17 04:01:00",
            "open": "4261.48",
            "high": "4261.48",
            "low": "4261.48",
            "close": "4261.48",
            "volume": "0.0",
            "close_time": "2017-08-17 04:01:59.999",
            "quote_asset_volume": "0.0",
            "number_of_trades": "0",
            "taker_buy_base_asset_volume": "0.0",
            "taker_buy_quote_asset_volume": "0.0",
            "ignore": "0",
        },
        {
            "timestamp": "2017-08-17 04:02:00",
            "open": "4280.56",
            "high": "4280.56",
            "low": "4280.56",
            "close": "4280.56",
            "volume": "-1.0",
            "close_time": "2017-08-17 04:02:59.999",
            "quote_asset_volume": "1117.54292144",
            "number_of_trades": "2",
            "taker_buy_base_asset_volume": "0.261074",
            "taker_buy_quote_asset_volume": "1117.54292144",
            "ignore": "0",
        },
    ]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    output_root = Path(tempfile.mkdtemp(prefix="kaggle_csv_to_bronze_test_"))
    csv_input = output_root / "BTCUSDT.csv"
    bronze_output = output_root / "bronze"

    try:
        write_sample_csv(csv_input)

        symbol = conversion_job.resolve_symbol("", str(csv_input))
        assert symbol == "BTCUSDT", f"expected BTCUSDT, got {symbol}"

        csv_df = conversion_job.load_csv_df(spark, str(csv_input))
        assert csv_df.count() == 3, "expected three CSV input rows"

        bronze_df = conversion_job.build_bronze_df(csv_df, symbol, "kaggle_csv", 1234567890000)
        assert bronze_df.count() == 2, "expected two valid Bronze rows after filtering invalid data"

        conversion_job.write_bronze_parquet(bronze_df, str(bronze_output), "overwrite")
        bronze_parquet_df = spark.read.parquet(str(bronze_output))

        expected_columns = {
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_time",
            "close_time",
            "is_closed",
            "source",
            "ingested_at",
            "year",
            "month",
        }
        assert set(bronze_parquet_df.columns) == expected_columns, bronze_parquet_df.columns
        assert bronze_parquet_df.count() == 2, "expected two Bronze parquet rows"

        distinct_sources = {row["source"] for row in bronze_parquet_df.select("source").distinct().collect()}
        assert distinct_sources == {"kaggle_csv"}, distinct_sources

        distinct_closed_flags = {row["is_closed"] for row in bronze_parquet_df.select("is_closed").distinct().collect()}
        assert distinct_closed_flags == {True}, distinct_closed_flags

        print("Verified Kaggle CSV to Bronze parquet conversion")
        print("kaggle_csv_to_bronze smoke test passed")
    finally:
        spark.stop()
        shutil.rmtree(output_root, ignore_errors=True)


if __name__ == "__main__":
    main()
