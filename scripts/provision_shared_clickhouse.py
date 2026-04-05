from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[1]
if not (REPO_ROOT / "sql" / "clickhouse").exists():
    REPO_ROOT = SCRIPT_PATH.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.jobs.clickhouse_client import ClickHouseConnection, run_clickhouse_query

DEFAULT_SQL_FILES = [
    REPO_ROOT / "sql" / "clickhouse" / "00_create_database.sql",
    REPO_ROOT / "sql" / "clickhouse" / "01_raw_ohlcv_1m.sql",
    REPO_ROOT / "sql" / "clickhouse" / "02_agg_ohlcv_1h.sql",
    REPO_ROOT / "sql" / "clickhouse" / "03_agg_ohlcv_1d.sql",
    REPO_ROOT / "sql" / "clickhouse" / "04_pipeline_metrics.sql",
    REPO_ROOT / "sql" / "clickhouse" / "06_technical_features.sql",
    REPO_ROOT / "sql" / "clickhouse" / "07_mv_technical_features_refresh.sql",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Provision the shared ClickHouse database and schema set for this pipeline."
    )
    parser.add_argument(
        "--clickhouse-url",
        default="http://3.234.211.100:8123",
        help="ClickHouse HTTP endpoint.",
    )
    parser.add_argument(
        "--database",
        default="is459_streaming_kj",
        help="Dedicated ClickHouse database to create and populate.",
    )
    return parser.parse_args()


def load_sql(path: Path, database: str) -> str:
    sql = path.read_text(encoding="utf-8")
    sql = sql.replace("CREATE DATABASE IF NOT EXISTS crypto;", f"CREATE DATABASE IF NOT EXISTS {database};")
    sql = sql.replace("crypto.", f"{database}.")
    return sql


def split_sql_statements(sql: str) -> list[str]:
    statements = []
    for chunk in sql.split(";"):
        statement = chunk.strip()
        if statement:
            statements.append(statement)
    return statements


def statement_params(path: Path, statement: str) -> dict[str, str] | None:
    if path.name == "07_mv_technical_features_refresh.sql":
        if statement.upper().startswith("SET "):
            return {}
        return {"allow_experimental_refreshable_materialized_view": "1"}
    return None


def main() -> None:
    args = parse_args()
    connection = ClickHouseConnection(url=args.clickhouse_url)

    for path in DEFAULT_SQL_FILES:
        for statement in split_sql_statements(load_sql(path, args.database)):
            params = statement_params(path, statement)
            if params == {}:
                continue
            run_clickhouse_query(connection, statement, params=params)
        print(f"Applied {path.name} to {args.database}")

    result = run_clickhouse_query(
        connection,
        (
            "SELECT name, engine FROM system.tables "
            f"WHERE database = '{args.database}' "
            "ORDER BY name FORMAT TabSeparated"
        ),
    ).strip()
    print(f"Provisioned database: {args.database}")
    print(result)


if __name__ == "__main__":
    main()
