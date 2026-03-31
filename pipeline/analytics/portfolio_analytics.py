"""
Pure portfolio analytics helpers consisting of finance and math logic used by Task 6.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import sqrt
import random
from typing import Iterable


@dataclass
class MarketData:
    symbols: list[str]
    close_dates: list[date]
    return_dates: list[date]
    closes_by_symbol: dict[str, list[float]]
    returns_matrix: list[list[float]]
    mean_returns: list[float]
    covariance_matrix: list[list[float]]
    correlation_matrix: list[list[float]]


@dataclass
class PortfolioSummary:
    avg_daily_return: float
    annualized_return: float
    portfolio_volatility: float
    annualized_volatility: float
    sharpe_ratio: float
    diversification_score: float
    diversification_label: str
    ending_value: float
    pct_gain: float


@dataclass
class BacktestPoint:
    date: date
    portfolio_value: float
    cumulative_return: float
    daily_return: float
    drawdown: float


@dataclass
class SimulationResult:
    simulation_id: int
    weights: list[float]
    annualized_return: float
    annualized_volatility: float
    sharpe_ratio: float
    diversification_score: float
    is_optimal: bool = False


def build_market_data(
    price_rows: Iterable[dict[str, object]],
    *,
    symbols: list[str],
    lookback_days: int,
    min_history_days: int,
) -> MarketData:
    by_symbol_date: dict[str, dict[date, float]] = {symbol: {} for symbol in symbols}

    for row in price_rows:
        symbol = str(row["symbol"])
        if symbol not in by_symbol_date:
            continue
        by_symbol_date[symbol][date.fromisoformat(str(row["date"]))] = float(row["close"])

    missing_symbols = [symbol for symbol, rows in by_symbol_date.items() if not rows]
    if missing_symbols:
        raise ValueError(f"Missing price history for symbols: {', '.join(missing_symbols)}")

    common_dates = set.intersection(*(set(rows.keys()) for rows in by_symbol_date.values()))
    ordered_dates = sorted(common_dates)
    if len(ordered_dates) < min_history_days + 1:
        raise ValueError(
            f"Need at least {min_history_days + 1} common dates, got {len(ordered_dates)}"
        )

    if len(ordered_dates) > lookback_days + 1:
        ordered_dates = ordered_dates[-(lookback_days + 1) :]

    closes_by_symbol: dict[str, list[float]] = {
        symbol: [by_symbol_date[symbol][day] for day in ordered_dates]
        for symbol in symbols
    }

    return_dates = ordered_dates[1:]
    returns_matrix: list[list[float]] = []
    for row_index in range(1, len(ordered_dates)):
        daily_returns = []
        for symbol in symbols:
            previous_close = closes_by_symbol[symbol][row_index - 1]
            current_close = closes_by_symbol[symbol][row_index]
            if previous_close <= 0:
                raise ValueError(f"Encountered non-positive close for {symbol}")
            daily_returns.append((current_close / previous_close) - 1.0)
        returns_matrix.append(daily_returns)

    mean_returns = _column_means(returns_matrix)
    covariance_matrix = _covariance_matrix(returns_matrix, mean_returns)
    correlation_matrix = _correlation_matrix(covariance_matrix)

    return MarketData(
        symbols=symbols,
        close_dates=ordered_dates,
        return_dates=return_dates,
        closes_by_symbol=closes_by_symbol,
        returns_matrix=returns_matrix,
        mean_returns=mean_returns,
        covariance_matrix=covariance_matrix,
        correlation_matrix=correlation_matrix,
    )


def build_weight_vector(
    symbols: list[str],
    sparse_weights: Iterable[tuple[str, float]],
) -> list[float]:
    weights_by_symbol = {symbol: float(weight) for symbol, weight in sparse_weights}
    return [weights_by_symbol.get(symbol, 0.0) for symbol in symbols]


def summarize_portfolio(
    *,
    weights: list[float],
    return_dates: list[date],
    returns_matrix: list[list[float]],
    correlation_matrix: list[list[float]],
    initial_capital: float,
    risk_free_rate: float,
    trading_days_per_year: int,
) -> tuple[PortfolioSummary, list[BacktestPoint]]:
    series = portfolio_return_series(weights, returns_matrix)
    backtest = build_backtest_series(
        return_dates=return_dates,
        daily_returns=series,
        initial_capital=initial_capital,
    )
    avg_daily_return = sum(series) / len(series)
    portfolio_volatility = _sample_stddev(series)
    annualized_return = (1 + avg_daily_return) ** trading_days_per_year - 1
    annualized_volatility = portfolio_volatility * sqrt(trading_days_per_year)
    sharpe_ratio = (
        (annualized_return - risk_free_rate) / annualized_volatility
        if annualized_volatility > 0
        else 0.0
    )
    diversification_score = diversification_score_for_weights(weights, correlation_matrix)
    diversification_label = diversification_label_for_score(diversification_score)
    ending_value = backtest[-1].portfolio_value
    pct_gain = (ending_value / initial_capital) - 1

    return (
        PortfolioSummary(
            avg_daily_return=avg_daily_return,
            annualized_return=annualized_return,
            portfolio_volatility=portfolio_volatility,
            annualized_volatility=annualized_volatility,
            sharpe_ratio=sharpe_ratio,
            diversification_score=diversification_score,
            diversification_label=diversification_label,
            ending_value=ending_value,
            pct_gain=pct_gain,
        ),
        backtest,
    )


def simulate_portfolios(
    *,
    symbols: list[str],
    returns_matrix: list[list[float]],
    mean_returns: list[float],
    covariance_matrix: list[list[float]],
    correlation_matrix: list[list[float]],
    simulation_count: int,
    random_seed: int,
    min_active_assets: int,
    max_active_assets: int,
    min_active_weight: float,
    risk_free_rate: float,
    trading_days_per_year: int,
) -> list[SimulationResult]:
    rng = random.Random(random_seed)
    simulations: list[SimulationResult] = []
    if min_active_weight < 0 or min_active_weight >= 1:
        raise ValueError("min_active_weight must be between 0 and 1")
    if min_active_assets < 1:
        raise ValueError("min_active_assets must be at least 1")
    if min_active_assets > max_active_assets:
        raise ValueError("min_active_assets cannot exceed max_active_assets")
    feasible_max_active = min(
        max_active_assets,
        len(symbols),
        int(1 / min_active_weight) if min_active_weight > 0 else len(symbols),
    )
    if feasible_max_active < 1:
        raise ValueError("No feasible active-asset count for the given constraints")
    feasible_min_active = min(min_active_assets, len(symbols))
    if feasible_min_active > feasible_max_active:
        raise ValueError("No feasible active-asset count for the given constraints")

    for simulation_id in range(1, simulation_count + 1):
        active_count = rng.randint(feasible_min_active, feasible_max_active)
        active_indices = rng.sample(range(len(symbols)), active_count)
        weights = [0.0 for _ in symbols]
        floor_weight = active_count * min_active_weight
        if floor_weight > 1.0 + 1e-12:
            raise ValueError("Portfolio weight floor exceeds 100%")
        remaining_weight = max(0.0, 1.0 - floor_weight)
        raw_weights = [rng.gammavariate(1.0, 1.0) for _ in range(active_count)]
        raw_total = sum(raw_weights)

        for index, symbol_index in enumerate(active_indices):
            extra_weight = (
                remaining_weight * (raw_weights[index] / raw_total)
                if raw_total > 0
                else 0.0
            )
            weights[symbol_index] = min_active_weight + extra_weight

        avg_daily_return = sum(
            weight * mean_return for weight, mean_return in zip(weights, mean_returns)
        )
        daily_volatility = portfolio_volatility_from_covariance(weights, covariance_matrix)
        annualized_return = (1 + avg_daily_return) ** trading_days_per_year - 1
        annualized_volatility = daily_volatility * sqrt(trading_days_per_year)
        sharpe_ratio = (
            (annualized_return - risk_free_rate) / annualized_volatility
            if annualized_volatility > 0
            else 0.0
        )

        simulations.append(
            SimulationResult(
                simulation_id=simulation_id,
                weights=weights,
                annualized_return=annualized_return,
                annualized_volatility=annualized_volatility,
                sharpe_ratio=sharpe_ratio,
                diversification_score=diversification_score_for_weights(
                    weights,
                    correlation_matrix,
                ),
            )
        )

    if simulations:
        best = max(simulations, key=lambda result: result.sharpe_ratio)
        best.is_optimal = True

    return simulations


def portfolio_return_series(weights: list[float], returns_matrix: list[list[float]]) -> list[float]:
    return [
        sum(weight * asset_return for weight, asset_return in zip(weights, day_returns))
        for day_returns in returns_matrix
    ]


def portfolio_volatility_from_covariance(
    weights: list[float],
    covariance_matrix: list[list[float]],
) -> float:
    variance = 0.0
    for row_index, row_weight in enumerate(weights):
        for col_index, col_weight in enumerate(weights):
            variance += row_weight * col_weight * covariance_matrix[row_index][col_index]
    return sqrt(max(variance, 0.0))


def build_backtest_series(
    *,
    return_dates: list[date],
    daily_returns: list[float],
    initial_capital: float,
) -> list[BacktestPoint]:
    portfolio_value = initial_capital
    running_peak = initial_capital
    series: list[BacktestPoint] = []

    for point_date, daily_return in zip(return_dates, daily_returns):
        portfolio_value *= 1 + daily_return
        running_peak = max(running_peak, portfolio_value)
        drawdown = (portfolio_value / running_peak) - 1 if running_peak else 0.0
        series.append(
            BacktestPoint(
                date=point_date,
                portfolio_value=portfolio_value,
                cumulative_return=(portfolio_value / initial_capital) - 1,
                daily_return=daily_return,
                drawdown=drawdown,
            )
        )
    return series


def diversification_score_for_weights(
    weights: list[float],
    correlation_matrix: list[list[float]],
) -> float:
    active_indices = [index for index, weight in enumerate(weights) if weight > 1e-6]
    if len(active_indices) <= 1:
        return 0.0

    pair_weight_sum = 0.0
    weighted_abs_correlation = 0.0
    for left_position, left_index in enumerate(active_indices):
        for right_index in active_indices[left_position + 1 :]:
            pair_weight = weights[left_index] * weights[right_index]
            pair_weight_sum += pair_weight
            weighted_abs_correlation += (
                pair_weight * abs(correlation_matrix[left_index][right_index])
            )

    avg_abs_correlation = (
        weighted_abs_correlation / pair_weight_sum if pair_weight_sum > 0 else 1.0
    )
    effective_holdings = 1.0 / sum(weight * weight for weight in weights if weight > 1e-12)
    normalized_holdings = min(1.0, effective_holdings / len(active_indices))
    score = (0.8 * (1.0 - avg_abs_correlation)) + (0.2 * normalized_holdings)
    return max(0.0, min(1.0, score))


def diversification_label_for_score(score: float) -> str:
    if score < 0.35:
        return "Low"
    if score < 0.65:
        return "Medium"
    return "High"


def _column_means(matrix: list[list[float]]) -> list[float]:
    row_count = len(matrix)
    column_count = len(matrix[0])
    return [
        sum(matrix[row][column] for row in range(row_count)) / row_count
        for column in range(column_count)
    ]


def _covariance_matrix(matrix: list[list[float]], means: list[float]) -> list[list[float]]:
    row_count = len(matrix)
    column_count = len(matrix[0])
    if row_count < 2:
        raise ValueError("Need at least two return rows to compute covariance")

    covariance = [[0.0 for _ in range(column_count)] for _ in range(column_count)]
    for row in matrix:
        diffs = [value - means[index] for index, value in enumerate(row)]
        for left in range(column_count):
            for right in range(left, column_count):
                covariance[left][right] += diffs[left] * diffs[right]

    divisor = row_count - 1
    for left in range(column_count):
        for right in range(left, column_count):
            value = covariance[left][right] / divisor
            covariance[left][right] = value
            covariance[right][left] = value

    return covariance


def _correlation_matrix(covariance_matrix: list[list[float]]) -> list[list[float]]:
    column_count = len(covariance_matrix)
    stddevs = [sqrt(max(covariance_matrix[index][index], 0.0)) for index in range(column_count)]
    correlation = [[0.0 for _ in range(column_count)] for _ in range(column_count)]

    for left in range(column_count):
        for right in range(column_count):
            if left == right:
                correlation[left][right] = 1.0
            elif stddevs[left] > 0 and stddevs[right] > 0:
                correlation[left][right] = (
                    covariance_matrix[left][right] / (stddevs[left] * stddevs[right])
                )
            else:
                correlation[left][right] = 0.0

    return correlation


def _sample_stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / (len(values) - 1)
    return sqrt(max(variance, 0.0))
