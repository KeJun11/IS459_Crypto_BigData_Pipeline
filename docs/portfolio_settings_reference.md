# Portfolio Settings Reference

This document explains the configuration values in
[`pipeline/config/portfolio_settings.py`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/config/portfolio_settings.py)
and how each value is used by
[`compute_portfolio_analytics.py`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/tasks/compute_portfolio_analytics.py).

The goal of this file is not just to list defaults, but to explain:

- what each setting controls
- where it is used in Task 6
- what changes when the value is increased or decreased
- what the setting does not control

## What `portfolio_settings.py` Is For

[`portfolio_settings.py`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/config/portfolio_settings.py)
contains the business and analytics assumptions for the portfolio optimization
layer.

It answers questions like:

- Who is the example investor?
- What is the baseline portfolio?
- How much history should be analyzed?
- How many Monte Carlo portfolios should be tested?
- How concentrated is the optimized portfolio allowed to be?

It does **not** hold infrastructure settings such as:

- ClickHouse host and port
- AWS settings
- S3 settings

Those remain in
[`settings.py`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/config/settings.py).

## How Task 6 Uses These Settings

At a high level, Task 6 does the following:

1. Reads daily close prices from `cleaned_crypto.ohlcv_1min`
2. Aligns the common daily history across all selected coins
3. Computes daily returns, covariance, and correlation
4. Evaluates the user-defined baseline portfolio
5. Generates Monte Carlo portfolios and scores them
6. Picks the best portfolio according to the optimization objective
7. Writes the results to ClickHouse tables used by Grafana

Every setting in this file influences one of those steps.

## 1. ClickHouse Output Table Settings

These values control **where** Task 6 writes its results.

### `CLICKHOUSE_PORTFOLIO_SCENARIOS_TABLE`

Default: `portfolio_scenarios`

Used for the scenario header row.

This table stores metadata such as:

- scenario id
- scenario name
- investor name
- baseline label
- benchmark symbol
- optimization objective
- analysis date range
- asset universe size

If you rename this setting, the actual output table name changes, but the
portfolio math does not.

### `CLICKHOUSE_PORTFOLIO_WEIGHTS_TABLE`

Default: `portfolio_weights`

Used for portfolio allocation outputs.

This table stores:

- baseline weights
- optimized weights
- the `simulation_id` that produced the optimized weights

Grafana donut charts and allocation tables should read from this table.

### `CLICKHOUSE_PORTFOLIO_METRICS_TABLE`

Default: `portfolio_metrics`

Used for KPI outputs.

This table stores one row per portfolio type such as:

- baseline
- optimized
- benchmark

Each row contains:

- Sharpe ratio
- average daily return
- annualized return
- daily volatility
- annualized volatility
- diversification score
- diversification label
- ending value
- percent gain

### `CLICKHOUSE_PORTFOLIO_CORRELATION_TABLE`

Default: `portfolio_correlation_matrix`

Used for the diversification heatmap.

Each row stores one pairwise correlation, for example:

- `BTCUSDT` vs `ETHUSDT`
- `BTCUSDT` vs `BTCUSDT`
- `ETHUSDT` vs `SOLUSDT`

If 48 coins are analyzed, this table contains `48 x 48 = 2304` rows.

### `CLICKHOUSE_PORTFOLIO_BACKTEST_TABLE`

Default: `portfolio_backtest_series`

Used for line charts comparing how each portfolio performed through time.

This table stores daily time-series points for:

- baseline portfolio
- optimized portfolio
- benchmark portfolio

### `CLICKHOUSE_PORTFOLIO_SIMULATIONS_TABLE`

Default: `portfolio_simulations`

Used for Monte Carlo summary outputs.

Each row is one simulation trial and stores:

- simulation id
- annualized return
- annualized volatility
- Sharpe ratio
- diversification score
- whether this simulation was selected as the optimal one

This table does **not** store the full weight vector for every simulation.
Only the winning optimized portfolio's weights are persisted in
`portfolio_weights`.

## 2. Scenario Definition Settings

These settings define the story behind the portfolio experiment.

### `PORTFOLIO_SCENARIO_ID`

Default: `jerry_10k_top50`

This is the main grouping key across all portfolio result tables.

Task 6 deletes old rows and reinserts fresh rows by `scenario_id`, so this
value effectively identifies one full analytics scenario.

If you changed this to `alice_25k_defensive`, Task 6 would create a separate
set of rows for that scenario instead of overwriting Jerry's results.

### `PORTFOLIO_SCENARIO_NAME`

Default: `Jerry 10k baseline vs optimized`

Human-readable scenario title.

This is mainly for metadata, reports, and dashboard display.

### `PORTFOLIO_INVESTOR_NAME`

Default: `Jerry`

Used as the example investor identity in the scenario metadata.

This does not affect the optimization result mathematically. It is purely part
of the storytelling layer.

### `PORTFOLIO_INITIAL_CAPITAL`

Default: `10000`

Starting dollar value of the portfolio backtest.

Task 6 uses this when converting percentage returns into portfolio values over
time.

Example:

- if a portfolio starts at `$10,000`
- and the cumulative return is `+20%`
- then ending value is about `$12,000`

Important nuance:

- this value affects `ending_value` and backtest portfolio-value lines
- it does **not** usually change which portfolio has the highest Sharpe ratio,
  because Sharpe is based on percentage return and percentage volatility

### `PORTFOLIO_BASELINE_LABEL`

Default: `50% BTC / 50% ETH`

Display label for the user-defined baseline portfolio.

This is the text you would likely show in Grafana next to the baseline
allocation chart or KPI card.

### `PORTFOLIO_BENCHMARK_SYMBOL`

Default: `BTCUSDT`

Defines the single-asset benchmark portfolio.

Task 6 builds a benchmark weight vector with:

- `100%` weight in `BTCUSDT`
- `0%` in all other coins

This is used to compare:

- baseline portfolio
- optimized portfolio
- simple "just buy BTC" strategy

### `PORTFOLIO_OPTIMIZATION_OBJECTIVE`

Default: `max_sharpe`

Defines what the optimizer is trying to maximize.

In the current code, the selected portfolio is the simulation with the highest
Sharpe ratio.

This setting is descriptive right now because the implementation only supports
that objective, but it is useful for documentation and future extension.

### `PORTFOLIO_REBALANCE_FREQUENCY`

Default: `daily`

This describes how the portfolio is treated in the backtest.

In the current implementation:

- the task works on daily returns
- portfolio returns are applied over the daily return series
- so the behavior is closest to a daily-rebalanced portfolio

This setting is currently a label rather than a switch that changes the
algorithm.

### `PORTFOLIO_RISK_FREE_RATE`

Default: `0`

Used in the Sharpe ratio formula:

```text
Sharpe = (annualized_return - risk_free_rate) / annualized_volatility
```

At `0`, the Sharpe ratio measures excess return over a zero-risk baseline.

If this value were increased:

- Sharpe ratios would generally fall
- portfolios would need higher return to look equally attractive

### `PORTFOLIO_BASELINE_WEIGHTS`

Default: `BTCUSDT:0.5,ETHUSDT:0.5`

Defines the manually chosen baseline portfolio as sparse weights.

Task 6 parses this string and converts it into a dense weight vector aligned
to the full asset universe.

In the current project, this means:

- `BTCUSDT = 50%`
- `ETHUSDT = 50%`
- every other available coin = `0%`

The weights must sum to exactly `1.0`.

## 3. Lookback and Data Quality Settings

These values decide how much price history is used and whether there is enough
usable data to trust the outputs.

### `PORTFOLIO_LOOKBACK_DAYS`

Default: `365`

This is the **final** number of daily observations the model tries to analyze.

Task 6 uses it to:

- choose the recent historical window
- compute daily returns
- estimate mean return, covariance, and correlation
- evaluate the baseline portfolio
- run the optimizer
- backtest the baseline, optimized, and benchmark portfolios

So in plain language:

- `365` means "build the optimizer using roughly the last one year of daily data"

If this value is increased:

- the model becomes more long-term
- recent trends matter less
- optimization may become more stable

If this value is decreased:

- the model becomes more sensitive to recent market behavior
- optimization may react more strongly to recent winners and losers

### `PORTFOLIO_LOOKBACK_BUFFER_DAYS`

Default: `45`

This is **not** the final analysis window. It is a fetch cushion.

Task 6 first tries to fetch:

```text
lookback_days + buffer_days
```

So with current values:

- `365 + 45 = 410` days are requested from ClickHouse first

Why this matters:

- some coins may be missing a few dates
- once the task intersects all symbols to get common dates, some days may drop
  out
- the extra 45 days makes it more likely that the final aligned history still
  contains a full 365-day usable window

This setting helps avoid unnecessary fallback to the full-history query.

### `PORTFOLIO_MIN_HISTORY_DAYS`

Default: `180`

This is the minimum acceptable number of aligned daily return observations.

It is a safeguard against weak analytics.

If, after aligning all selected coins, the common history is shorter than this
threshold, Task 6 raises an error instead of producing unreliable portfolio
metrics.

In practical terms:

- if there are too few overlapping dates
- the covariance and correlation estimates become noisy
- the optimizer could choose portfolios based on unstable data

So `PORTFOLIO_MIN_HISTORY_DAYS` is a quality control check.

## 4. Monte Carlo Search Settings

These values control how the optimizer explores candidate portfolios.

### `PORTFOLIO_SIMULATION_COUNT`

Default: `5000`

This is the number of random portfolios generated and scored.

Important clarification:

- Monte Carlo does **not** test every possible portfolio
- it samples the portfolio space randomly

So `5000` means:

- generate 5000 candidate portfolios
- compute return, volatility, Sharpe, and diversification for each
- keep the best one found

If this value is increased:

- search coverage improves
- the optimizer has more chances to find a better portfolio
- runtime increases

If this value is too small:

- results may be less stable
- a better portfolio might exist in the feasible space but never get sampled

### `PORTFOLIO_RANDOM_SEED`

Default: `42`

This fixes the random number generator state for reproducibility.

With the same:

- dataset
- constraints
- simulation count
- random seed

the optimizer should generate the same sequence of random portfolios and pick
the same winner.

This setting is useful for:

- debugging
- report reproducibility
- comparing code changes fairly

The value `42` has no finance meaning. It is simply a conventional seed value.

## 5. Practical Allocation Constraints

These values shape what kinds of optimized portfolios are allowed.

### `PORTFOLIO_MIN_ACTIVE_COINS`

Default: `5`

This is the minimum number of coins that must receive a non-zero weight in
each simulated portfolio.

Why it was added:

- without this constraint, the optimizer was allowed to choose a one-coin
  portfolio
- that caused the max-Sharpe result to collapse into `100% ZECUSDT`
- the Sharpe ratio jumped unrealistically high because the model was no longer
  balancing return against diversification in a practical way

With the current value:

- every simulated portfolio must hold at least 5 assets

### `PORTFOLIO_MAX_ACTIVE_COINS`

Default: `10`

This is the maximum number of coins that may receive non-zero weight.

Purpose:

- prevents the optimizer from spreading across too many tiny holdings
- makes the final portfolio easier to explain visually
- keeps the dashboard more realistic for a human investor

So with the current setting:

- the optimized portfolio cannot contain 20 or 30 tiny positions
- it must stay between 5 and 10 active assets

### `PORTFOLIO_MIN_ACTIVE_WEIGHT`

Default: `0.05`

This means any selected asset must have at least `5%` allocation.

Why it matters:

- avoids dust allocations like `0.1%`, `0.2%`, `0.5%`
- makes the optimized portfolio readable and actionable

With the current constraint combination:

- minimum active coins = `5`
- maximum active coins = `10`
- minimum active weight = `5%`

the optimizer searches for portfolios that:

- hold between 5 and 10 assets
- allocate at least 5% to each selected asset

Important interaction:

- if you select 5 assets and each must be at least 5%, then 25% of the
  portfolio is already fixed as minimum allocation
- the remaining 75% is what Monte Carlo distributes flexibly

## 6. Annualization Setting

### `PORTFOLIO_TRADING_DAYS_PER_YEAR`

Default: `252`

This converts daily metrics into annualized metrics.

Task 6 uses it for:

```text
annualized_return = (1 + avg_daily_return) ^ trading_days_per_year - 1
annualized_volatility = daily_volatility * sqrt(trading_days_per_year)
```

With the current value:

- daily return estimates are projected to a standard trading-year scale
- volatility is scaled consistently for Sharpe calculation

Although crypto trades 24/7, `252` is still a common simplification for
finance-style comparison. If changed, annualized return, annualized
volatility, and Sharpe ratio will all change.

## 7. Asset Universe Setting

### `ASSET_UNIVERSE`

Current source: `settings.BINANCE_SYMBOLS`

This defines which symbols the optimizer is allowed to consider.

Task 6 then intersects that list with the symbols actually present in
ClickHouse.

So the true analysis universe is:

```text
configured symbols ∩ available symbols in ClickHouse
```

Example from the current project:

- `EOSUSDT` and `STMXUSDT` were configured in the broader symbol list
- but not present in ClickHouse
- so Task 6 skipped them automatically

This is why the correlation matrix ended up with `48 x 48` rows instead of 50.

## 8. How These Settings Affect Current Outputs

With the current configuration, the project is modeling:

- investor: Jerry
- starting capital: `$10,000`
- baseline portfolio: `50% BTC / 50% ETH`
- benchmark: `100% BTC`
- analysis window: roughly the last 1 year of daily data
- optimization objective: max Sharpe
- practical optimizer constraints:
  - at least 5 coins
  - at most 10 coins
  - at least 5% per selected coin

That means the optimized portfolio is no longer a purely theoretical,
fully unconstrained optimum. It is now a more presentation-friendly
"practical max-Sharpe portfolio."

## 9. What To Change If The Results Look Wrong

### If the optimized portfolio is too concentrated

Change one or more of these:

- increase `PORTFOLIO_MIN_ACTIVE_COINS`
- add a future `max single coin weight` constraint
- increase `PORTFOLIO_MIN_ACTIVE_WEIGHT` only if the portfolio still holds too
  many dust positions

### If the optimized portfolio is too fragmented

Change one or more of these:

- decrease `PORTFOLIO_MAX_ACTIVE_COINS`
- increase `PORTFOLIO_MIN_ACTIVE_WEIGHT`

### If the optimizer feels too reactive to recent market conditions

Consider:

- increasing `PORTFOLIO_LOOKBACK_DAYS`

### If the optimizer feels too slow

Consider:

- decreasing `PORTFOLIO_SIMULATION_COUNT`

### If the optimizer feels unstable between runs

Consider:

- increasing `PORTFOLIO_SIMULATION_COUNT`
- keeping `PORTFOLIO_RANDOM_SEED` fixed

### If Task 6 fails due to insufficient aligned history

Consider:

- reducing `PORTFOLIO_MIN_HISTORY_DAYS`
- reducing `PORTFOLIO_LOOKBACK_DAYS`
- shrinking the asset universe to coins with stronger historical overlap
