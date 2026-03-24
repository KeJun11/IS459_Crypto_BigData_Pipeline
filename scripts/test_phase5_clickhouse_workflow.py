from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test the Phase 5 ClickHouse materialized view workflow."
    )
    parser.add_argument(
        "--clickhouse-url",
        default="http://localhost:8123",
        help="ClickHouse HTTP endpoint.",
    )
    parser.add_argument(
        "--docker-container",
        default="",
        help="Optional ClickHouse container name. When set, queries run via docker exec clickhouse-client.",
    )
    parser.add_argument(
        "--database",
        default="crypto_phase5_smoke",
        help="Temporary database used by the smoke test.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=90,
        help="Maximum time to wait for the refreshable materialized view to populate rows.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=5,
        help="Polling interval while waiting for the materialized view refresh.",
    )
    parser.add_argument(
        "--keep-database",
        action="store_true",
        help="Keep the temporary database after the test passes.",
    )
    return parser.parse_args()


def run_clickhouse_query(
    clickhouse_url: str,
    query: str,
    data: bytes | None = None,
    docker_container: str = "",
    multiquery: bool = False,
) -> str:
    if docker_container:
        command = [
            "docker",
            "exec",
            "-i",
            docker_container,
            "clickhouse-client",
        ]
        if multiquery:
            command.append("--multiquery")
        command.extend(["--query", query])
        try:
            result = subprocess.run(
                command,
                input=data,
                capture_output=True,
                check=False,
            )
        except OSError as exc:
            raise RuntimeError(f"Failed to execute docker query command: {exc}") from exc
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse docker query failed: {stderr}")
        return result.stdout.decode("utf-8")

    request = urllib.request.Request(
        url=f"{clickhouse_url}/?query={urllib.parse.quote(query)}",
        data=data,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse query failed ({exc.code}): {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to reach ClickHouse at {clickhouse_url}: {exc}") from exc


def load_sql(path: Path, database: str) -> str:
    sql = path.read_text(encoding="utf-8")
    sql = sql.replace("CREATE DATABASE IF NOT EXISTS crypto;", f"CREATE DATABASE IF NOT EXISTS {database};")
    sql = sql.replace("crypto.", f"{database}.")
    return sql


def prepare_schema(clickhouse_url: str, database: str, docker_container: str) -> None:
    run_clickhouse_query(clickhouse_url, f"DROP DATABASE IF EXISTS {database} SYNC", docker_container=docker_container)

    sql_files = [
        REPO_ROOT / "sql" / "clickhouse" / "00_create_database.sql",
        REPO_ROOT / "sql" / "clickhouse" / "01_raw_ohlcv_1m.sql",
        REPO_ROOT / "sql" / "clickhouse" / "06_technical_features.sql",
        REPO_ROOT / "sql" / "clickhouse" / "07_mv_technical_features_refresh.sql",
    ]
    for path in sql_files:
        run_clickhouse_query(
            clickhouse_url,
            load_sql(path, database),
            docker_container=docker_container,
            multiquery=True,
        )


def build_sample_rows() -> list[dict[str, object]]:
    start = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
    closes = [
        100.0,
        101.0,
        102.0,
        103.0,
        104.0,
        105.0,
        106.0,
        107.0,
        108.0,
        109.0,
        110.0,
        111.0,
        112.0,
        113.0,
        114.0,
    ]

    rows: list[dict[str, object]] = []
    for index, close_value in enumerate(closes):
        open_time = start + timedelta(minutes=index)
        close_time = open_time + timedelta(seconds=59, milliseconds=999)
        rows.append(
            {
                "symbol": "BTCUSDT",
                "open_time": open_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "close_time": close_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "open": close_value - 0.5,
                "high": close_value + 0.5,
                "low": close_value - 1.0,
                "close": close_value,
                "volume": 10.0 + index,
                "is_closed": 1,
                "source": "phase5_smoke_test",
            }
        )
    return rows


def insert_sample_rows(clickhouse_url: str, database: str, rows: list[dict[str, object]], docker_container: str) -> None:
    payload = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows).encode("utf-8")
    query = f"INSERT INTO {database}.raw_ohlcv_1m FORMAT JSONEachRow"
    run_clickhouse_query(clickhouse_url, query, data=payload, docker_container=docker_container)


def fetch_feature_rows(clickhouse_url: str, database: str, docker_container: str) -> list[dict[str, object]]:
    query = f"""
    SELECT
        symbol,
        open_time,
        close,
        return_1m,
        ma_5,
        ma_15,
        volatility_15
    FROM {database}.technical_features
    WHERE symbol = 'BTCUSDT'
    ORDER BY open_time
    FORMAT JSON
    """
    response = run_clickhouse_query(clickhouse_url, query, docker_container=docker_container)
    return json.loads(response)["data"]


def wait_for_refresh(
    clickhouse_url: str,
    database: str,
    expected_rows: int,
    timeout_seconds: int,
    poll_seconds: int,
    docker_container: str,
) -> list[dict[str, object]]:
    deadline = time.time() + timeout_seconds
    latest_rows: list[dict[str, object]] = []
    while time.time() < deadline:
        latest_rows = fetch_feature_rows(clickhouse_url, database, docker_container)
        if len(latest_rows) >= expected_rows:
            return latest_rows
        time.sleep(poll_seconds)
    raise RuntimeError(
        f"Materialized view did not populate {expected_rows} row(s) within {timeout_seconds} seconds; found {len(latest_rows)}"
    )


def assert_close(actual: float | None, expected: float, tolerance: float, label: str) -> None:
    if actual is None:
        raise RuntimeError(f"{label} was NULL, expected {expected}")
    if abs(actual - expected) > tolerance:
        raise RuntimeError(f"{label} was {actual}, expected {expected}")


def verify_rows(rows: list[dict[str, object]]) -> None:
    if len(rows) != 15:
        raise RuntimeError(f"Expected 15 feature rows, found {len(rows)}")

    first_row = rows[0]
    second_row = rows[1]
    fifth_row = rows[4]
    fifteenth_row = rows[14]

    if first_row["return_1m"] is not None:
        raise RuntimeError("First row return_1m should be NULL because no prior candle exists")

    assert_close(float(second_row["return_1m"]), 0.01, 1e-9, "Second row return_1m")
    assert_close(float(fifth_row["ma_5"]), 102.0, 1e-9, "Fifth row ma_5")
    assert_close(float(fifteenth_row["ma_15"]), 107.0, 1e-9, "Fifteenth row ma_15")

    volatility = fifteenth_row["volatility_15"]
    if volatility is None or float(volatility) <= 0:
        raise RuntimeError(f"Expected fifteenth row volatility_15 to be positive, found {volatility}")


def main() -> None:
    args = parse_args()
    sample_rows = build_sample_rows()

    try:
        prepare_schema(args.clickhouse_url, args.database, args.docker_container)
        insert_sample_rows(args.clickhouse_url, args.database, sample_rows, args.docker_container)
        feature_rows = wait_for_refresh(
            args.clickhouse_url,
            args.database,
            expected_rows=len(sample_rows),
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
            docker_container=args.docker_container,
        )
        verify_rows(feature_rows)
        print(f"Verified refreshable materialized view in database: {args.database}")
        print("Verified return_1m, ma_5, ma_15, and volatility_15 outputs")
        print("Phase 5 ClickHouse workflow smoke test passed")
    finally:
        if not args.keep_database:
            try:
                run_clickhouse_query(
                    args.clickhouse_url,
                    f"DROP DATABASE IF EXISTS {args.database} SYNC",
                    docker_container=args.docker_container,
                )
            except RuntimeError as exc:
                print(f"Warning: failed to drop temporary database {args.database}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
