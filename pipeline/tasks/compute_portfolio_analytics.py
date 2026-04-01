"""
Task 6 - compute_portfolio_analytics
====================================
Compute portfolio analytics from ClickHouse market data and store the
dashboard-ready outputs back into ClickHouse.
"""

from datetime import date, timedelta
import json
import re
from typing import Iterable

import requests

from pipeline.config import portfolio_settings
from pipeline.config import settings
from pipeline.analytics.portfolio_analytics import (
    BacktestPoint,
    PortfolioSummary,
    SimulationResult,
    build_market_data,
    build_weight_vector,
    simulate_portfolios,
    summarize_portfolio,
)
from pipeline.utils.logger import get_logger

log = get_logger("task6.compute_portfolio_analytics")

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, value: str) -> str:
    if not value or not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{name} must contain only letters, numbers, and underscores")
    return value


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _clickhouse_base_url() -> str:
    return (
        f"{settings.CLICKHOUSE_PROTOCOL}://"
        f"{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}"
    )


def _auth() -> tuple[str, str]:
    return (settings.CLICKHOUSE_USER, settings.CLICKHOUSE_PASSWORD)


def _session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    return session


def _execute(query: str, *, label: str) -> str:
    with _session() as session:
        response = session.post(
            f"{_clickhouse_base_url()}/",
            auth=_auth(),
            data=query.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=settings.CLICKHOUSE_HTTP_TIMEOUT,
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        detail = response.text.strip() or response.reason
        raise RuntimeError(f"ClickHouse query failed during {label}: {detail}") from exc
    return response.text.strip()


def _execute_json_each_row(query: str, *, label: str) -> list[dict[str, object]]:
    payload = _execute(query, label=label)
    if not payload:
        return []
    return [json.loads(line) for line in payload.splitlines() if line.strip()]


def _ping_clickhouse() -> None:
    with _session() as session:
        response = session.get(
            f"{_clickhouse_base_url()}/ping",
            auth=_auth(),
            timeout=10,
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError("ClickHouse ping failed") from exc

    if response.text.strip() != "Ok.":
        raise RuntimeError(f"Unexpected ClickHouse ping response: {response.text.strip()}")


def _parse_baseline_weights(value: str) -> list[tuple[str, float]]:
    weights: list[tuple[str, float]] = []
    if not value.strip():
        raise ValueError("PORTFOLIO_BASELINE_WEIGHTS cannot be empty")

    for item in value.split(","):
        symbol_part, weight_part = item.split(":", maxsplit=1)
        symbol = symbol_part.strip().upper()
        weight = float(weight_part.strip())
        if not symbol:
            raise ValueError("Baseline weight symbol cannot be empty")
        if weight < 0:
            raise ValueError("Baseline weights cannot be negative")
        weights.append((symbol, weight))

    total_weight = sum(weight for _, weight in weights)
    if abs(total_weight - 1.0) > 1e-9:
        raise ValueError(
            f"PORTFOLIO_BASELINE_WEIGHTS must sum to 1.0, got {total_weight:.6f}"
        )
    return weights


def _create_database_query(database: str) -> str:
    return f"CREATE DATABASE IF NOT EXISTS {database}"


def _row_count_query(table_ref: str) -> str:
    return f"SELECT count() FROM {table_ref}"


def _max_market_date_query(table_ref: str) -> str:
    return f"SELECT toString(max(date)) FROM {table_ref}"


def _available_symbols_query(table_ref: str) -> str:
    return f"""
    SELECT symbol
    FROM {table_ref}
    GROUP BY symbol
    ORDER BY symbol
    FORMAT JSONEachRow
    """


def _daily_close_query(
    table_ref: str,
    *,
    symbols: list[str],
    start_date: date | None,
    end_date: date | None,
) -> str:
    symbol_list = ", ".join(_sql_string(symbol) for symbol in symbols)
    filters = [f"symbol IN ({symbol_list})"]
    if start_date:
        filters.append(
            f"toDate(timestamp) >= toDate({_sql_string(start_date.isoformat())})"
        )
    if end_date:
        filters.append(f"toDate(timestamp) <= toDate({_sql_string(end_date.isoformat())})")

    where_sql = " AND ".join(filters)
    return f"""
    SELECT
        symbol,
        toString(toDate(timestamp)) AS date,
        argMax(close, timestamp) AS close
    FROM {table_ref}
    WHERE {where_sql}
    GROUP BY symbol, toDate(timestamp)
    ORDER BY date, symbol
    FORMAT JSONEachRow
    """


def _create_portfolio_scenarios_query(table_ref: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref}
    (
        scenario_id String,
        scenario_name String,
        investor_name String,
        initial_capital Float64,
        baseline_label String,
        benchmark_symbol LowCardinality(String),
        optimization_objective LowCardinality(String),
        rebalance_frequency LowCardinality(String),
        risk_free_rate Float64,
        training_start_date Nullable(Date),
        training_end_date Nullable(Date),
        backtest_start_date Nullable(Date),
        backtest_end_date Nullable(Date),
        asset_universe_size UInt16,
        created_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(created_at)
    ORDER BY scenario_id
    """


def _create_portfolio_weights_query(table_ref: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref}
    (
        scenario_id String,
        portfolio_type LowCardinality(String),
        simulation_id Nullable(UInt32),
        symbol LowCardinality(String),
        weight Float64,
        weight_rank UInt16,
        notes String DEFAULT '',
        created_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(created_at)
    ORDER BY (scenario_id, portfolio_type, symbol)
    """


def _ensure_portfolio_weights_schema_query(table_ref: str) -> str:
    return (
        f"ALTER TABLE {table_ref} "
        "ADD COLUMN IF NOT EXISTS simulation_id Nullable(UInt32) "
        "AFTER portfolio_type"
    )


def _create_portfolio_metrics_query(table_ref: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref}
    (
        scenario_id String,
        portfolio_type LowCardinality(String),
        as_of_date Date,
        sharpe_ratio Float64,
        avg_daily_return Float64,
        annualized_return Float64,
        portfolio_volatility Float64,
        annualized_volatility Float64,
        diversification_score Float64,
        diversification_label LowCardinality(String),
        ending_value Float64,
        pct_gain Float64,
        created_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(created_at)
    PARTITION BY toYYYYMM(as_of_date)
    ORDER BY (scenario_id, portfolio_type, as_of_date)
    """


def _create_portfolio_correlation_query(table_ref: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref}
    (
        scenario_id String,
        as_of_date Date,
        symbol_x LowCardinality(String),
        symbol_y LowCardinality(String),
        correlation Float64,
        abs_correlation Float64 MATERIALIZED abs(correlation),
        created_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(created_at)
    PARTITION BY toYYYYMM(as_of_date)
    ORDER BY (scenario_id, as_of_date, symbol_x, symbol_y)
    """


def _create_portfolio_backtest_query(table_ref: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref}
    (
        scenario_id String,
        portfolio_type LowCardinality(String),
        date Date,
        portfolio_value Float64,
        cumulative_return Float64,
        daily_return Float64,
        drawdown Float64,
        benchmark_value Nullable(Float64),
        created_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(created_at)
    PARTITION BY toYYYYMM(date)
    ORDER BY (scenario_id, portfolio_type, date)
    """


def _create_portfolio_simulations_query(table_ref: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref}
    (
        scenario_id String,
        simulation_id UInt32,
        annualized_return Float64,
        annualized_volatility Float64,
        sharpe_ratio Float64,
        diversification_score Float64,
        is_optimal UInt8,
        created_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(created_at)
    ORDER BY (scenario_id, simulation_id)
    """


def _delete_existing_scenario_query(table_ref: str, scenario_id: str) -> str:
    return (
        f"DELETE FROM {table_ref} "
        f"WHERE scenario_id = {_sql_string(scenario_id)} "
        "SETTINGS mutations_sync = 1"
    )


def _insert_scenario_query(
    table_ref: str,
    *,
    scenario_id: str,
    training_start_date: date,
    training_end_date: date,
    backtest_start_date: date,
    backtest_end_date: date,
    asset_universe_size: int,
) -> str:
    return f"""
    INSERT INTO {table_ref}
    (
        scenario_id,
        scenario_name,
        investor_name,
        initial_capital,
        baseline_label,
        benchmark_symbol,
        optimization_objective,
        rebalance_frequency,
        risk_free_rate,
        training_start_date,
        training_end_date,
        backtest_start_date,
        backtest_end_date,
        asset_universe_size
    )
    VALUES
    (
        {_sql_string(scenario_id)},
        {_sql_string(portfolio_settings.PORTFOLIO_SCENARIO_NAME)},
        {_sql_string(portfolio_settings.PORTFOLIO_INVESTOR_NAME)},
        {portfolio_settings.PORTFOLIO_INITIAL_CAPITAL},
        {_sql_string(portfolio_settings.PORTFOLIO_BASELINE_LABEL)},
        {_sql_string(portfolio_settings.PORTFOLIO_BENCHMARK_SYMBOL)},
        {_sql_string(portfolio_settings.PORTFOLIO_OPTIMIZATION_OBJECTIVE)},
        {_sql_string(portfolio_settings.PORTFOLIO_REBALANCE_FREQUENCY)},
        {portfolio_settings.PORTFOLIO_RISK_FREE_RATE},
        toDate({_sql_string(training_start_date.isoformat())}),
        toDate({_sql_string(training_end_date.isoformat())}),
        toDate({_sql_string(backtest_start_date.isoformat())}),
        toDate({_sql_string(backtest_end_date.isoformat())}),
        {asset_universe_size}
    )
    """


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _insert_rows(
    *,
    prefix: str,
    values: list[str],
    label: str,
    chunk_size: int = 1000,
) -> None:
    if not values:
        return
    for index, batch in enumerate(_chunked(values, chunk_size), start=1):
        _execute(prefix + ",\n".join(batch), label=f"{label} chunk {index}")


def _dense_to_ranked_weights(
    symbols: list[str],
    weights: list[float],
) -> list[tuple[str, float]]:
    return sorted(
        (
            (symbol, weight)
            for symbol, weight in zip(symbols, weights)
            if weight > 1e-8
        ),
        key=lambda item: item[1],
        reverse=True,
    )


def _weight_value_rows(
    scenario_id: str,
    portfolio_type: str,
    simulation_id: int | None,
    rows: list[tuple[str, float]],
    note: str,
) -> list[str]:
    return [
        "("
        f"{_sql_string(scenario_id)}, "
        f"{_sql_string(portfolio_type)}, "
        f"{'NULL' if simulation_id is None else simulation_id}, "
        f"{_sql_string(symbol)}, "
        f"{weight}, "
        f"{rank}, "
        f"{_sql_string(note)}"
        ")"
        for rank, (symbol, weight) in enumerate(rows, start=1)
    ]


def _metric_value_rows(
    scenario_id: str,
    as_of_date: date,
    rows: list[tuple[str, PortfolioSummary]],
) -> list[str]:
    return [
        "("
        f"{_sql_string(scenario_id)}, "
        f"{_sql_string(portfolio_type)}, "
        f"toDate({_sql_string(as_of_date.isoformat())}), "
        f"{summary.sharpe_ratio}, "
        f"{summary.avg_daily_return}, "
        f"{summary.annualized_return}, "
        f"{summary.portfolio_volatility}, "
        f"{summary.annualized_volatility}, "
        f"{summary.diversification_score}, "
        f"{_sql_string(summary.diversification_label)}, "
        f"{summary.ending_value}, "
        f"{summary.pct_gain}"
        ")"
        for portfolio_type, summary in rows
    ]


def _correlation_value_rows(
    scenario_id: str,
    as_of_date: date,
    symbols: list[str],
    correlation_matrix: list[list[float]],
) -> list[str]:
    rows: list[str] = []
    for row_index, symbol_x in enumerate(symbols):
        for col_index, symbol_y in enumerate(symbols):
            rows.append(
                "("
                f"{_sql_string(scenario_id)}, "
                f"toDate({_sql_string(as_of_date.isoformat())}), "
                f"{_sql_string(symbol_x)}, "
                f"{_sql_string(symbol_y)}, "
                f"{correlation_matrix[row_index][col_index]}"
                ")"
            )
    return rows


def _backtest_value_rows(
    scenario_id: str,
    portfolio_type: str,
    series: list[BacktestPoint],
    benchmark_values_by_date: dict[date, float] | None,
) -> list[str]:
    rows = []
    for point in series:
        benchmark_value = (
            benchmark_values_by_date.get(point.date)
            if benchmark_values_by_date is not None
            else None
        )
        benchmark_sql = "NULL" if benchmark_value is None else str(benchmark_value)
        rows.append(
            "("
            f"{_sql_string(scenario_id)}, "
            f"{_sql_string(portfolio_type)}, "
            f"toDate({_sql_string(point.date.isoformat())}), "
            f"{point.portfolio_value}, "
            f"{point.cumulative_return}, "
            f"{point.daily_return}, "
            f"{point.drawdown}, "
            f"{benchmark_sql}"
            ")"
        )
    return rows


def _simulation_value_rows(
    scenario_id: str,
    simulations: list[SimulationResult],
) -> list[str]:
    return [
        "("
        f"{_sql_string(scenario_id)}, "
        f"{simulation.simulation_id}, "
        f"{simulation.annualized_return}, "
        f"{simulation.annualized_volatility}, "
        f"{simulation.sharpe_ratio}, "
        f"{simulation.diversification_score}, "
        f"{1 if simulation.is_optimal else 0}"
        ")"
        for simulation in simulations
    ]


def _fetch_market_data(raw_table_ref: str, symbols: list[str]):
    max_market_date = date.fromisoformat(
        _execute(_max_market_date_query(raw_table_ref), label="read max market date")
    )
    recent_start_date = max_market_date - timedelta(
        days=portfolio_settings.PORTFOLIO_LOOKBACK_DAYS
        + portfolio_settings.PORTFOLIO_LOOKBACK_BUFFER_DAYS
    )
    recent_rows = _execute_json_each_row(
        _daily_close_query(
            raw_table_ref,
            symbols=symbols,
            start_date=recent_start_date,
            end_date=max_market_date,
        ),
        label="fetch recent daily closes",
    )
    try:
        market_data = build_market_data(
            recent_rows,
            symbols=symbols,
            lookback_days=portfolio_settings.PORTFOLIO_LOOKBACK_DAYS,
            min_history_days=portfolio_settings.PORTFOLIO_MIN_HISTORY_DAYS,
        )
        log.info(
            "Using recent daily history from %s to %s (%d return days)",
            market_data.close_dates[0],
            market_data.close_dates[-1],
            len(market_data.return_dates),
        )
        return market_data
    except ValueError as exc:
        log.info("Recent history window was insufficient (%s). Retrying with full history.", exc)

    full_rows = _execute_json_each_row(
        _daily_close_query(
            raw_table_ref,
            symbols=symbols,
            start_date=None,
            end_date=max_market_date,
        ),
        label="fetch full daily closes",
    )
    market_data = build_market_data(
        full_rows,
        symbols=symbols,
        lookback_days=portfolio_settings.PORTFOLIO_LOOKBACK_DAYS,
        min_history_days=portfolio_settings.PORTFOLIO_MIN_HISTORY_DAYS,
    )
    log.info(
        "Using full-history fallback from %s to %s (%d return days)",
        market_data.close_dates[0],
        market_data.close_dates[-1],
        len(market_data.return_dates),
    )
    return market_data


def compute_portfolio_analytics() -> dict[str, int]:
    database = _validate_identifier("CLICKHOUSE_DB", settings.CLICKHOUSE_DB)
    raw_table = _validate_identifier(
        "CLICKHOUSE_RAW_OHLCV_TABLE",
        settings.CLICKHOUSE_RAW_OHLCV_TABLE,
    )
    scenario_table = _validate_identifier(
        "CLICKHOUSE_PORTFOLIO_SCENARIOS_TABLE",
        portfolio_settings.CLICKHOUSE_PORTFOLIO_SCENARIOS_TABLE,
    )
    weights_table = _validate_identifier(
        "CLICKHOUSE_PORTFOLIO_WEIGHTS_TABLE",
        portfolio_settings.CLICKHOUSE_PORTFOLIO_WEIGHTS_TABLE,
    )
    metrics_table = _validate_identifier(
        "CLICKHOUSE_PORTFOLIO_METRICS_TABLE",
        portfolio_settings.CLICKHOUSE_PORTFOLIO_METRICS_TABLE,
    )
    correlation_table = _validate_identifier(
        "CLICKHOUSE_PORTFOLIO_CORRELATION_TABLE",
        portfolio_settings.CLICKHOUSE_PORTFOLIO_CORRELATION_TABLE,
    )
    backtest_table = _validate_identifier(
        "CLICKHOUSE_PORTFOLIO_BACKTEST_TABLE",
        portfolio_settings.CLICKHOUSE_PORTFOLIO_BACKTEST_TABLE,
    )
    simulations_table = _validate_identifier(
        "CLICKHOUSE_PORTFOLIO_SIMULATIONS_TABLE",
        portfolio_settings.CLICKHOUSE_PORTFOLIO_SIMULATIONS_TABLE,
    )

    raw_table_ref = f"{database}.{raw_table}"
    scenario_ref = f"{database}.{scenario_table}"
    weights_ref = f"{database}.{weights_table}"
    metrics_ref = f"{database}.{metrics_table}"
    correlation_ref = f"{database}.{correlation_table}"
    backtest_ref = f"{database}.{backtest_table}"
    simulations_ref = f"{database}.{simulations_table}"
    baseline_sparse_weights = _parse_baseline_weights(
        portfolio_settings.PORTFOLIO_BASELINE_WEIGHTS
    )

    log.info("=== Task 6: compute_portfolio_analytics START ===")
    log.info("Target ClickHouse: %s", _clickhouse_base_url())
    log.info("Scenario id: %s", portfolio_settings.PORTFOLIO_SCENARIO_ID)
    log.info("Database: %s", database)

    _ping_clickhouse()
    log.info("ClickHouse ping successful")

    _execute(_create_database_query(database), label="create database")
    _execute(_create_portfolio_scenarios_query(scenario_ref), label="create scenarios table")
    _execute(_create_portfolio_weights_query(weights_ref), label="create weights table")
    _execute(
        _ensure_portfolio_weights_schema_query(weights_ref),
        label="ensure weights simulation_id column",
    )
    _execute(_create_portfolio_metrics_query(metrics_ref), label="create metrics table")
    _execute(
        _create_portfolio_correlation_query(correlation_ref),
        label="create correlation table",
    )
    _execute(_create_portfolio_backtest_query(backtest_ref), label="create backtest table")
    _execute(
        _create_portfolio_simulations_query(simulations_ref),
        label="create simulations table",
    )

    available_symbols = [
        str(row["symbol"])
        for row in _execute_json_each_row(
            _available_symbols_query(raw_table_ref),
            label="fetch available symbols",
        )
    ]
    analysis_symbols = [
        symbol for symbol in portfolio_settings.ASSET_UNIVERSE if symbol in available_symbols
    ]
    missing_symbols = [
        symbol for symbol in portfolio_settings.ASSET_UNIVERSE if symbol not in available_symbols
    ]
    if missing_symbols:
        log.info(
            "Skipping configured symbols that are not present in ClickHouse: %s",
            ", ".join(missing_symbols),
        )
    if portfolio_settings.PORTFOLIO_BENCHMARK_SYMBOL not in analysis_symbols:
        raise RuntimeError(
            f"Benchmark symbol {portfolio_settings.PORTFOLIO_BENCHMARK_SYMBOL} is missing"
        )

    market_data = _fetch_market_data(raw_table_ref, analysis_symbols)
    baseline_weights = build_weight_vector(market_data.symbols, baseline_sparse_weights)
    benchmark_weights = build_weight_vector(
        market_data.symbols,
        [(portfolio_settings.PORTFOLIO_BENCHMARK_SYMBOL, 1.0)],
    )
    simulations = simulate_portfolios(
        symbols=market_data.symbols,
        returns_matrix=market_data.returns_matrix,
        mean_returns=market_data.mean_returns,
        covariance_matrix=market_data.covariance_matrix,
        correlation_matrix=market_data.correlation_matrix,
        simulation_count=portfolio_settings.PORTFOLIO_SIMULATION_COUNT,
        random_seed=portfolio_settings.PORTFOLIO_RANDOM_SEED,
        min_active_assets=portfolio_settings.PORTFOLIO_MIN_ACTIVE_COINS,
        max_active_assets=portfolio_settings.PORTFOLIO_MAX_ACTIVE_COINS,
        min_active_weight=portfolio_settings.PORTFOLIO_MIN_ACTIVE_WEIGHT,
        risk_free_rate=portfolio_settings.PORTFOLIO_RISK_FREE_RATE,
        trading_days_per_year=portfolio_settings.PORTFOLIO_TRADING_DAYS_PER_YEAR,
    )
    if not simulations:
        raise RuntimeError("No Monte Carlo simulations were produced")
    best_simulation = next(simulation for simulation in simulations if simulation.is_optimal)
    optimized_weights = best_simulation.weights

    baseline_summary, baseline_backtest = summarize_portfolio(
        weights=baseline_weights,
        return_dates=market_data.return_dates,
        returns_matrix=market_data.returns_matrix,
        correlation_matrix=market_data.correlation_matrix,
        initial_capital=portfolio_settings.PORTFOLIO_INITIAL_CAPITAL,
        risk_free_rate=portfolio_settings.PORTFOLIO_RISK_FREE_RATE,
        trading_days_per_year=portfolio_settings.PORTFOLIO_TRADING_DAYS_PER_YEAR,
    )
    optimized_summary, optimized_backtest = summarize_portfolio(
        weights=optimized_weights,
        return_dates=market_data.return_dates,
        returns_matrix=market_data.returns_matrix,
        correlation_matrix=market_data.correlation_matrix,
        initial_capital=portfolio_settings.PORTFOLIO_INITIAL_CAPITAL,
        risk_free_rate=portfolio_settings.PORTFOLIO_RISK_FREE_RATE,
        trading_days_per_year=portfolio_settings.PORTFOLIO_TRADING_DAYS_PER_YEAR,
    )
    benchmark_summary, benchmark_backtest = summarize_portfolio(
        weights=benchmark_weights,
        return_dates=market_data.return_dates,
        returns_matrix=market_data.returns_matrix,
        correlation_matrix=market_data.correlation_matrix,
        initial_capital=portfolio_settings.PORTFOLIO_INITIAL_CAPITAL,
        risk_free_rate=portfolio_settings.PORTFOLIO_RISK_FREE_RATE,
        trading_days_per_year=portfolio_settings.PORTFOLIO_TRADING_DAYS_PER_YEAR,
    )

    scenario_id = portfolio_settings.PORTFOLIO_SCENARIO_ID
    as_of_date = market_data.return_dates[-1]
    benchmark_values_by_date = {
        point.date: point.portfolio_value for point in benchmark_backtest
    }

    for table_ref, label in (
        (scenario_ref, "clear scenario"),
        (weights_ref, "clear weights"),
        (metrics_ref, "clear metrics"),
        (correlation_ref, "clear correlation"),
        (backtest_ref, "clear backtest"),
        (simulations_ref, "clear simulations"),
    ):
        _execute(_delete_existing_scenario_query(table_ref, scenario_id), label=label)

    _execute(
        _insert_scenario_query(
            scenario_ref,
            scenario_id=scenario_id,
            training_start_date=market_data.close_dates[0],
            training_end_date=market_data.close_dates[-1],
            backtest_start_date=market_data.return_dates[0],
            backtest_end_date=market_data.return_dates[-1],
            asset_universe_size=len(market_data.symbols),
        ),
        label="insert scenario",
    )

    _insert_rows(
        prefix=f"""
        INSERT INTO {weights_ref}
        (
            scenario_id,
            portfolio_type,
            simulation_id,
            symbol,
            weight,
            weight_rank,
            notes
        )
        VALUES
        """,
        values=_weight_value_rows(
            scenario_id,
            "baseline",
            None,
            _dense_to_ranked_weights(market_data.symbols, baseline_weights),
            "User-defined baseline portfolio",
        )
        + _weight_value_rows(
            scenario_id,
            "optimized",
            best_simulation.simulation_id,
            _dense_to_ranked_weights(market_data.symbols, optimized_weights),
            "Monte Carlo max-sharpe portfolio",
        ),
        label="insert weights",
    )

    _insert_rows(
        prefix=f"""
        INSERT INTO {metrics_ref}
        (
            scenario_id,
            portfolio_type,
            as_of_date,
            sharpe_ratio,
            avg_daily_return,
            annualized_return,
            portfolio_volatility,
            annualized_volatility,
            diversification_score,
            diversification_label,
            ending_value,
            pct_gain
        )
        VALUES
        """,
        values=_metric_value_rows(
            scenario_id,
            as_of_date,
            [
                ("baseline", baseline_summary),
                ("optimized", optimized_summary),
                ("benchmark", benchmark_summary),
            ],
        ),
        label="insert metrics",
    )

    _insert_rows(
        prefix=f"""
        INSERT INTO {correlation_ref}
        (
            scenario_id,
            as_of_date,
            symbol_x,
            symbol_y,
            correlation
        )
        VALUES
        """,
        values=_correlation_value_rows(
            scenario_id,
            as_of_date,
            market_data.symbols,
            market_data.correlation_matrix,
        ),
        label="insert correlations",
        chunk_size=500,
    )

    _insert_rows(
        prefix=f"""
        INSERT INTO {backtest_ref}
        (
            scenario_id,
            portfolio_type,
            date,
            portfolio_value,
            cumulative_return,
            daily_return,
            drawdown,
            benchmark_value
        )
        VALUES
        """,
        values=_backtest_value_rows(
            scenario_id,
            "baseline",
            baseline_backtest,
            benchmark_values_by_date,
        )
        + _backtest_value_rows(
            scenario_id,
            "optimized",
            optimized_backtest,
            benchmark_values_by_date,
        )
        + _backtest_value_rows(
            scenario_id,
            "benchmark",
            benchmark_backtest,
            None,
        ),
        label="insert backtest",
        chunk_size=500,
    )

    _insert_rows(
        prefix=f"""
        INSERT INTO {simulations_ref}
        (
            scenario_id,
            simulation_id,
            annualized_return,
            annualized_volatility,
            sharpe_ratio,
            diversification_score,
            is_optimal
        )
        VALUES
        """,
        values=_simulation_value_rows(scenario_id, simulations),
        label="insert simulations",
        chunk_size=500,
    )

    counts = {
        "scenarios": int(_execute(_row_count_query(scenario_ref), label="count scenarios")),
        "weights": int(_execute(_row_count_query(weights_ref), label="count weights")),
        "metrics": int(_execute(_row_count_query(metrics_ref), label="count metrics")),
        "correlations": int(
            _execute(_row_count_query(correlation_ref), label="count correlations")
        ),
        "backtest_points": int(
            _execute(_row_count_query(backtest_ref), label="count backtest points")
        ),
        "simulations": int(
            _execute(_row_count_query(simulations_ref), label="count simulations")
        ),
    }

    log.info(
        "Baseline Sharpe %.3f vs optimized Sharpe %.3f",
        baseline_summary.sharpe_ratio,
        optimized_summary.sharpe_ratio,
    )
    log.info(
        "Baseline ending value %.2f vs optimized ending value %.2f",
        baseline_summary.ending_value,
        optimized_summary.ending_value,
    )
    log.info("=== Task 6: compute_portfolio_analytics DONE === %s", counts)
    return counts


if __name__ == "__main__":
    summary = compute_portfolio_analytics()
    print(summary)
