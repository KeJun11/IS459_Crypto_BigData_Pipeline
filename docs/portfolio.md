# Portfolio Analytics Overview

This document summarizes how the portfolio optimization layer works in this
project and how its outputs are used in Grafana.

For detailed formulas, see
[`portfolio_calculations_reference.md`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/docs/portfolio_calculations_reference.md).
For configuration details, see
[`portfolio_settings_reference.md`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/docs/portfolio_settings_reference.md).

## Purpose

Task 6, implemented in
[`compute_portfolio_analytics.py`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/tasks/compute_portfolio_analytics.py),
takes cleaned crypto price data from ClickHouse and turns it into
dashboard-ready portfolio analytics.

The goal is to compare three portfolios:

- current portfolio: Jerry's baseline allocation
- optimized portfolio: the best Monte Carlo portfolio under the configured
  constraints
- benchmark portfolio: a simple `100% BTC` reference

## Current Scenario

The current default scenario is defined in
[`portfolio_settings.py`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/config/portfolio_settings.py):

- `scenario_id`: `jerry_10k_top50`
- investor: `Jerry`
- starting capital: `$10,000`
- baseline portfolio: `50% BTC / 50% ETH`
- benchmark portfolio: `100% BTC`
- optimization objective: `max_sharpe`
- lookback window: `365` days
- Monte Carlo simulations: `50,000`
- practical constraints:
  - minimum active coins: `5`
  - maximum active coins: `15`
  - minimum active weight per selected coin: `5%`

## Task 6 Flow

At a high level, Task 6 performs the following steps:

1. Read minute-level market data from
   [`ohlcv_1min`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/tasks/load_to_clickhouse.py).
2. Aggregate it to daily close prices in ClickHouse.
3. Align common daily history across the selected asset universe.
4. Compute daily returns, covariance, and correlation.
5. Evaluate the baseline portfolio.
6. Generate and score Monte Carlo portfolios.
7. Select the highest-Sharpe feasible portfolio as the optimized portfolio.
8. Build backtest series for baseline, optimized, and benchmark portfolios.
9. Write all outputs back into ClickHouse for Grafana to query.

## ClickHouse Output Tables

Task 6 writes to six portfolio result tables in the `cleaned_crypto` database.

### `portfolio_scenarios`

Stores scenario-level metadata such as:

- scenario id and scenario name
- investor name
- initial capital
- baseline label
- benchmark symbol
- optimization objective
- analysis window used
- size of the usable asset universe

### `portfolio_weights`

Stores the allocations used in dashboard donut charts.

It contains:

- baseline weights
- optimized weights
- `simulation_id` for the winning optimized portfolio

### `portfolio_metrics`

Stores one KPI row per portfolio type:

- `baseline`
- `optimized`
- `benchmark`

Important columns include:

- `sharpe_ratio`
- `avg_daily_return`
- `annualized_return`
- `portfolio_volatility`
- `annualized_volatility`
- `diversification_score`
- `diversification_label`
- `ending_value`
- `pct_gain`

### `portfolio_correlation_matrix`

Stores pairwise asset correlations derived from aligned daily returns.

Each row represents one pair:

- `symbol_x`
- `symbol_y`
- `correlation`

This table powers the Grafana correlation matrix.

### `portfolio_backtest_series`

Stores the historical value path for each portfolio over the backtest window.

Important columns include:

- `date`
- `portfolio_type`
- `portfolio_value`
- `daily_return`
- `cumulative_return`
- `drawdown`

This table powers the historical comparison line chart.

### `portfolio_simulations`

Stores one row per Monte Carlo trial.

Important columns include:

- `simulation_id`
- `annualized_return`
- `annualized_volatility`
- `sharpe_ratio`
- `diversification_score`
- `is_optimal`

This table is useful for explaining how the optimizer searched the feasible
portfolio space.

## Mental Model For Each Grafana Section

Grafana is not doing the finance math from scratch. It is mainly reading
precomputed results from the portfolio tables above.

### Current Portfolio

This section describes Jerry's original portfolio:

- `50% BTC`
- `50% ETH`

Panels and source columns:

- Current Portfolio Allocation
  - table: `portfolio_weights`
  - filter: `portfolio_type = 'baseline'`
  - columns: `symbol`, `weight`
- Sharpe Ratio
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'baseline'`
  - column: `sharpe_ratio`
- Avg Daily Return
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'baseline'`
  - column: `avg_daily_return`
- Portfolio Volatility
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'baseline'`
  - column: `annualized_volatility`
- Diversification Score
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'baseline'`
  - columns: `diversification_label`, `diversification_score`

### Optimized Portfolio

This section describes the best feasible Monte Carlo portfolio under the
current settings.

Panels and source columns:

- Optimized Portfolio Allocation
  - table: `portfolio_weights`
  - filter: `portfolio_type = 'optimized'`
  - columns: `symbol`, `weight`, `simulation_id`
- Sharpe Ratio
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'optimized'`
  - column: `sharpe_ratio`
- Avg Daily Return
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'optimized'`
  - column: `avg_daily_return`
- Portfolio Volatility
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'optimized'`
  - column: `annualized_volatility`
- Diversification Score
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'optimized'`
  - columns: `diversification_label`, `diversification_score`
- Ending Value / Percentage Gain
  - table: `portfolio_metrics`
  - filter: `portfolio_type = 'optimized'`
  - columns: `ending_value`, `pct_gain`

### Backtested Results - Optimized vs Current Portfolio

This chart compares the value path of:

- current portfolio = `baseline`
- optimized portfolio = `optimized`
- BTC-only reference = `benchmark`

It uses:

- table: `portfolio_backtest_series`
- columns: `date`, `portfolio_type`, `portfolio_value`

Interpretation:

- `portfolio_value` answers: "If Jerry started with `$10,000`, how much would
  the portfolio be worth on each day of the backtest?"
- `pct_gain` answers: "What was the total percentage gain or loss by the end
  of the backtest window?"
- the backtest currently uses roughly `365` daily observations based on the
  configured lookback window

### Correlation Matrix

This section explains diversification by showing how assets move together.

It uses:

- table: `portfolio_correlation_matrix`
- columns: `symbol_x`, `symbol_y`, `correlation`

Interpretation:

- high correlation means two assets tend to move together
- lower correlation suggests better diversification potential

For presentation, the matrix can be filtered to the most important optimized
coins plus `BTCUSDT` and `ETHUSDT`.

## Main Calculations Behind The Dashboard

The key portfolio calculations used by the dashboard are:

- Daily return per asset
  - `(close_t / close_(t-1)) - 1`
- Portfolio daily return
  - weighted sum of asset returns
- Average daily return
  - mean of portfolio daily returns
- Annualized volatility
  - daily volatility scaled by `sqrt(252)`
- Sharpe ratio
  - `(annualized_return - risk_free_rate) / annualized_volatility`
- Diversification score
  - custom heuristic based on correlation and breadth of holdings
- Ending value
  - final portfolio value at the end of the backtest
- Percentage gain
  - `(ending_value / initial_capital) - 1`
- Correlation
  - pairwise correlation of aligned daily return series

## Why ClickHouse Helps

The heavy raw dataset lives in ClickHouse, but Task 6 does not optimize
directly on every minute-level row. Instead, it:

- queries daily close prices from ClickHouse
- computes portfolio analytics on the smaller daily dataset
- writes compact result tables back into ClickHouse

This makes the portfolio layer much faster than the initial raw-data load and
makes Grafana panels simpler, because each panel can query a purpose-built
results table instead of doing complex financial calculations inside the
dashboard.

## Summary

The portfolio workflow in this project is:

1. Load raw crypto data into ClickHouse.
2. Compute daily portfolio analytics in Task 6.
3. Store the results in dedicated portfolio tables.
4. Use Grafana to visualize baseline, optimized, benchmark, backtest, and
   correlation outputs.

This separation keeps the system easier to explain:

- ClickHouse acts as the analytics serving layer
- Task 6 performs the portfolio math
- Grafana acts as the dashboard and presentation layer
