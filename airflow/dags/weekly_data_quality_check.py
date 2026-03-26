from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

from _common import (
    CLICKHOUSE_DATABASE,
    DEFAULT_DAG_ARGS,
    TRACKED_SYMBOLS,
    build_clickhouse_interval_filter,
    clickhouse_scalar,
    require_runtime_settings,
)


with DAG(
    dag_id="weekly_data_quality_check",
    description="Weekly ClickHouse data quality scan for gaps, duplicates, and out-of-range candles.",
    default_args=DEFAULT_DAG_ARGS,
    start_date=datetime(2026, 3, 23),
    schedule="0 3 * * 1",
    catchup=False,
    tags=["crypto", "phase7", "data-quality"],
    render_template_as_native_obj=True,
) as dag:
    @task(task_id="validate_runtime_settings")
    def validate_runtime_settings() -> None:
        require_runtime_settings()

    @task(task_id="run_quality_checks")
    def run_quality_checks() -> dict:
        context = get_current_context()
        interval_end = context["data_interval_end"]
        interval_start = interval_end - timedelta(days=7)
        interval_start_str = interval_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        interval_end_str = interval_end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        interval_filter = build_clickhouse_interval_filter("open_time", interval_start_str, interval_end_str)

        duplicate_group_count = int(
            clickhouse_scalar(
                f"SELECT count() FROM ("
                f"SELECT symbol, open_time, count() AS row_count "
                f"FROM {CLICKHOUSE_DATABASE}.raw_ohlcv_1m "
                f"WHERE {interval_filter} "
                f"GROUP BY symbol, open_time "
                f"HAVING row_count > 1)"
            )
        )
        out_of_range_count = int(
            clickhouse_scalar(
                f"SELECT count() FROM {CLICKHOUSE_DATABASE}.raw_ohlcv_1m "
                f"WHERE {interval_filter} AND ("
                f"low > high OR open > high OR open < low OR close > high OR close < low OR volume < 0)"
            )
        )

        expected_minutes = max(int((interval_end - interval_start).total_seconds() // 60), 0)
        symbol_gap_counts: dict[str, int] = {}
        for symbol in TRACKED_SYMBOLS:
            actual_count = int(
                clickhouse_scalar(
                    f"SELECT count(DISTINCT open_time) FROM {CLICKHOUSE_DATABASE}.raw_ohlcv_1m FINAL "
                    f"WHERE symbol = '{symbol}' AND {interval_filter}"
                )
            )
            symbol_gap_counts[symbol] = max(expected_minutes - actual_count, 0)

        return {
            "window_start": interval_start_str,
            "window_end": interval_end_str,
            "duplicate_group_count": duplicate_group_count,
            "out_of_range_count": out_of_range_count,
            "gap_counts_by_symbol": symbol_gap_counts,
        }

    @task(task_id="validate_quality_results")
    def validate_quality_results(quality_results: dict) -> dict:
        total_gap_count = sum(int(value) for value in quality_results["gap_counts_by_symbol"].values())
        quality_results["total_gap_count"] = total_gap_count
        if quality_results["duplicate_group_count"] > 0:
            raise ValueError(f"Duplicate groups found: {quality_results['duplicate_group_count']}")
        if quality_results["out_of_range_count"] > 0:
            raise ValueError(f"Out-of-range candles found: {quality_results['out_of_range_count']}")
        if total_gap_count > 0:
            raise ValueError(f"Missing 1-minute intervals found: {total_gap_count}")
        return quality_results

    quality_results = run_quality_checks()
    validate_runtime_settings() >> quality_results
    quality_results >> validate_quality_results(quality_results)
