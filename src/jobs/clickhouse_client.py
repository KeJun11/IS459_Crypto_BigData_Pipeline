from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class ClickHouseConnection:
    url: str = "http://localhost:8123"
    docker_container: str = ""


def run_clickhouse_query(
    connection: ClickHouseConnection,
    query: str,
    data: bytes | None = None,
    multiquery: bool = False,
) -> str:
    if connection.docker_container:
        command = [
            "docker",
            "exec",
            "-i",
            connection.docker_container,
            "clickhouse-client",
        ]
        if multiquery:
            command.append("--multiquery")
        command.extend(["--query", query])
        result = subprocess.run(
            command,
            input=data,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse docker query failed: {stderr}")
        return result.stdout.decode("utf-8")

    request = urllib.request.Request(
        url=f"{connection.url}/?query={urllib.parse.quote(query)}",
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
        raise RuntimeError(f"Failed to reach ClickHouse at {connection.url}: {exc}") from exc


def insert_json_each_row(
    connection: ClickHouseConnection,
    database: str,
    table: str,
    rows: list[dict],
) -> None:
    if not rows:
        return
    payload = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows).encode("utf-8")
    query = f"INSERT INTO {database}.{table} FORMAT JSONEachRow"
    run_clickhouse_query(connection, query, data=payload)
