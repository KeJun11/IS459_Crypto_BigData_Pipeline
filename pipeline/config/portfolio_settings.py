"""
Portfolio analytics configuration
=================================
Scenario-specific settings for the optimization and Grafana analytics layer.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

from pipeline.config import settings

# Load .env from project root (one level up from config/)
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")


CLICKHOUSE_PORTFOLIO_SCENARIOS_TABLE = os.getenv(
    "CLICKHOUSE_PORTFOLIO_SCENARIOS_TABLE",
    "portfolio_scenarios",
)
CLICKHOUSE_PORTFOLIO_WEIGHTS_TABLE = os.getenv(
    "CLICKHOUSE_PORTFOLIO_WEIGHTS_TABLE",
    "portfolio_weights",
)
CLICKHOUSE_PORTFOLIO_METRICS_TABLE = os.getenv(
    "CLICKHOUSE_PORTFOLIO_METRICS_TABLE",
    "portfolio_metrics",
)
CLICKHOUSE_PORTFOLIO_CORRELATION_TABLE = os.getenv(
    "CLICKHOUSE_PORTFOLIO_CORRELATION_TABLE",
    "portfolio_correlation_matrix",
)
CLICKHOUSE_PORTFOLIO_BACKTEST_TABLE = os.getenv(
    "CLICKHOUSE_PORTFOLIO_BACKTEST_TABLE",
    "portfolio_backtest_series",
)
CLICKHOUSE_PORTFOLIO_SIMULATIONS_TABLE = os.getenv(
    "CLICKHOUSE_PORTFOLIO_SIMULATIONS_TABLE",
    "portfolio_simulations",
)

PORTFOLIO_SCENARIO_ID = os.getenv("PORTFOLIO_SCENARIO_ID", "jerry_10k_top50")
PORTFOLIO_SCENARIO_NAME = os.getenv(
    "PORTFOLIO_SCENARIO_NAME",
    "Jerry 10k baseline vs optimized",
)
PORTFOLIO_INVESTOR_NAME = os.getenv("PORTFOLIO_INVESTOR_NAME", "Jerry")
PORTFOLIO_INITIAL_CAPITAL = float(os.getenv("PORTFOLIO_INITIAL_CAPITAL", "10000"))
PORTFOLIO_BASELINE_LABEL = os.getenv(
    "PORTFOLIO_BASELINE_LABEL",
    "50% BTC / 50% ETH",
)
PORTFOLIO_BENCHMARK_SYMBOL = os.getenv("PORTFOLIO_BENCHMARK_SYMBOL", "BTCUSDT")
PORTFOLIO_OPTIMIZATION_OBJECTIVE = os.getenv(
    "PORTFOLIO_OPTIMIZATION_OBJECTIVE",
    "max_sharpe",
)
PORTFOLIO_REBALANCE_FREQUENCY = os.getenv(
    "PORTFOLIO_REBALANCE_FREQUENCY",
    "daily",
)
PORTFOLIO_RISK_FREE_RATE = float(os.getenv("PORTFOLIO_RISK_FREE_RATE", "0"))
PORTFOLIO_BASELINE_WEIGHTS = os.getenv(
    "PORTFOLIO_BASELINE_WEIGHTS",
    "BTCUSDT:0.5,ETHUSDT:0.5",
)
PORTFOLIO_LOOKBACK_DAYS = int(os.getenv("PORTFOLIO_LOOKBACK_DAYS", "365")) 
PORTFOLIO_LOOKBACK_BUFFER_DAYS = int(
    os.getenv("PORTFOLIO_LOOKBACK_BUFFER_DAYS", "45")
)
PORTFOLIO_MIN_HISTORY_DAYS = int(os.getenv("PORTFOLIO_MIN_HISTORY_DAYS", "180"))
PORTFOLIO_SIMULATION_COUNT = int(os.getenv("PORTFOLIO_SIMULATION_COUNT", "10000"))
PORTFOLIO_RANDOM_SEED = int(os.getenv("PORTFOLIO_RANDOM_SEED", "42"))
PORTFOLIO_MIN_ACTIVE_COINS = int(os.getenv("PORTFOLIO_MIN_ACTIVE_COINS", "5"))
PORTFOLIO_MAX_ACTIVE_COINS = int(os.getenv("PORTFOLIO_MAX_ACTIVE_COINS", "15"))
PORTFOLIO_MIN_ACTIVE_WEIGHT = float(os.getenv("PORTFOLIO_MIN_ACTIVE_WEIGHT", "0.10"))
PORTFOLIO_TRADING_DAYS_PER_YEAR = int(
    os.getenv("PORTFOLIO_TRADING_DAYS_PER_YEAR", "252")
)

ASSET_UNIVERSE = settings.BINANCE_SYMBOLS
