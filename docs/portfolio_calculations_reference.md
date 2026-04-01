# Portfolio Calculations Reference

This document explains the main portfolio calculations used in
[`pipeline/analytics/portfolio_analytics.py`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/analytics/portfolio_analytics.py)
and
[`pipeline/tasks/compute_portfolio_analytics.py`](/c:/big_data/Crypto_Pipeline_Grp4/IS459_Crypto_BigData_Pipeline/pipeline/tasks/compute_portfolio_analytics.py).

These calculations are used to produce the portfolio analytics tables stored in
ClickHouse and visualized in Grafana.

## 1. Daily Return Per Asset

Each asset's daily return is computed from consecutive daily close prices.

Formula:

```text
daily_return_t = (close_t / close_(t-1)) - 1
```

Meaning:

- if yesterday's close was `100`
- and today's close is `105`
- then daily return is `(105 / 100) - 1 = 0.05 = 5%`

This calculation is applied to all assets after their daily close history has
been aligned to common dates.

## 2. Portfolio Daily Return

Once asset-level daily returns are available, the portfolio daily return is
computed as the weighted sum of asset returns.

Formula:

```text
portfolio_daily_return_t = sum(weight_i * asset_return_(i,t))
```

Where:

- `weight_i` is the portfolio allocation to asset `i`
- `asset_return_(i,t)` is the daily return of asset `i` on day `t`

Example:

- BTC weight = `0.5`
- ETH weight = `0.5`
- BTC daily return = `2%`
- ETH daily return = `-1%`

Then:

```text
portfolio_daily_return = (0.5 * 0.02) + (0.5 * -0.01) = 0.005 = 0.5%
```

## 3. Average Daily Return

The average daily return is the arithmetic mean of all portfolio daily returns
over the backtest window.

Formula:

```text
avg_daily_return = sum(portfolio_daily_returns) / number_of_days
```

This is the value stored in `portfolio_metrics.avg_daily_return`.

## 4. Sample Standard Deviation of Portfolio Returns

Portfolio volatility at the daily level is measured using the sample standard
deviation of the portfolio daily return series.

Formula:

```text
portfolio_volatility = sqrt(
    sum((r_t - mean_r)^2) / (n - 1)
)
```

Where:

- `r_t` is the portfolio daily return on day `t`
- `mean_r` is the average daily return
- `n` is the number of observations

This is the value stored in `portfolio_metrics.portfolio_volatility`.

## 5. Annualized Return

The annualized return converts the average daily return into a yearly
equivalent using compounding.

Formula:

```text
annualized_return = (1 + avg_daily_return) ^ trading_days_per_year - 1
```

Current setting:

- `trading_days_per_year = 252`

Example:

- if `avg_daily_return = 0.001`
- then:

```text
annualized_return = (1.001)^252 - 1
```

This is stored in `portfolio_metrics.annualized_return`.

## 6. Annualized Volatility

Annualized volatility scales daily volatility to a yearly basis.

Formula:

```text
annualized_volatility = portfolio_volatility * sqrt(trading_days_per_year)
```

Current setting:

- `trading_days_per_year = 252`

This is stored in `portfolio_metrics.annualized_volatility`.

## 7. Sharpe Ratio

The Sharpe ratio measures risk-adjusted return.

Formula:

```text
sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility
```

Current project setting:

- `risk_free_rate = 0`

Interpretation:

- higher Sharpe ratio means better return per unit of risk
- a portfolio can have high volatility but still have a high Sharpe if its
  return is high enough relative to that volatility

This is stored in `portfolio_metrics.sharpe_ratio`.

## 8. Covariance Matrix

The covariance matrix measures how asset returns move together.

For two assets `i` and `j`, covariance is estimated from aligned daily return
series.

Formula:

```text
cov(i,j) = sum((r_i - mean_i) * (r_j - mean_j)) / (n - 1)
```

Where:

- `r_i` is return series for asset `i`
- `mean_i` is mean return of asset `i`
- `n` is number of observations

The covariance matrix is used for:

- portfolio volatility estimation
- correlation matrix construction

## 9. Portfolio Volatility From Covariance Matrix

Before building the portfolio return series, the optimizer can estimate daily
portfolio volatility using the covariance matrix and the weight vector.

Formula:

```text
portfolio_variance = w^T * Cov * w
portfolio_volatility = sqrt(portfolio_variance)
```

Expanded form:

```text
portfolio_variance = sum_i sum_j (w_i * w_j * cov(i,j))
```

Where:

- `w_i` is the weight of asset `i`
- `cov(i,j)` is covariance between assets `i` and `j`

This is how each Monte Carlo simulation is scored before the best portfolio is
selected.

## 10. Correlation Coefficient

Correlation standardizes covariance so it lies between `-1` and `1`.

Formula:

```text
corr(i,j) = cov(i,j) / (std_i * std_j)
```

Where:

- `std_i` is the standard deviation of asset `i`
- `std_j` is the standard deviation of asset `j`

Interpretation:

- `1` = perfectly positively correlated
- `0` = no linear relationship
- `-1` = perfectly negatively correlated

These values are stored in `portfolio_correlation_matrix`.

## 11. Diversification Score

The diversification score is a custom metric in this project. It is not a
standard finance formula, but a blended heuristic combining:

- low average absolute correlation
- breadth of holdings

### Step 1: compute weighted average absolute correlation

For all active asset pairs:

```text
weighted_abs_correlation =
    sum(pair_weight * abs(corr(i,j))) / sum(pair_weight)
```

Where:

```text
pair_weight = weight_i * weight_j
```

### Step 2: compute effective holdings

Formula:

```text
effective_holdings = 1 / sum(weight_i^2)
```

This is larger when the portfolio is more evenly distributed.

### Step 3: normalize holdings

Formula:

```text
normalized_holdings = min(1, effective_holdings / active_asset_count)
```

### Step 4: blend the two components

Formula used in the code:

```text
diversification_score =
    0.8 * (1 - avg_abs_correlation) +
    0.2 * normalized_holdings
```

The result is then clipped between `0` and `1`.

Interpretation:

- closer to `1` = more diversified
- closer to `0` = less diversified

This is stored in `portfolio_metrics.diversification_score`.

## 12. Diversification Label

The numeric diversification score is mapped into a label for easier display in
Grafana.

Current thresholds:

```text
score < 0.35   -> Low
score < 0.65   -> Medium
otherwise      -> High
```

This is stored in `portfolio_metrics.diversification_label`.

## 13. Portfolio Value Through Time

Backtest portfolio value is computed by applying daily returns to the running
portfolio value.

Formula:

```text
portfolio_value_t = portfolio_value_(t-1) * (1 + daily_return_t)
```

Initial value:

```text
portfolio_value_0 = initial_capital
```

Example:

- initial capital = `$10,000`
- day 1 return = `+2%`
- day 2 return = `-1%`

Then:

```text
day 1 value = 10000 * 1.02 = 10200
day 2 value = 10200 * 0.99 = 10098
```

These values are stored in `portfolio_backtest_series.portfolio_value`.

## 14. Cumulative Return

Cumulative return measures total growth from the initial capital.

Formula:

```text
cumulative_return = (portfolio_value / initial_capital) - 1
```

If:

- initial capital = `$10,000`
- current portfolio value = `$12,500`

Then:

```text
cumulative_return = (12500 / 10000) - 1 = 0.25 = 25%
```

This is stored in `portfolio_backtest_series.cumulative_return`.

## 15. Ending Value

Ending value is simply the final portfolio value at the end of the backtest
window.

Formula:

```text
ending_value = last(portfolio_value_t)
```

This is stored in `portfolio_metrics.ending_value`.

## 16. Percentage Gain (`pct_gain`)

`pct_gain` measures total gain or loss from the starting capital to the ending
value.

Formula:

```text
pct_gain = (ending_value / initial_capital) - 1
```

Example:

- initial capital = `$10,000`
- ending value = `$31,547.35`

Then:

```text
pct_gain = (31547.35 / 10000) - 1
         = 2.154735
         = 215.47%
```

Important note:

- `pct_gain` is a decimal in the database
- Grafana usually formats it as a percentage

This is stored in `portfolio_metrics.pct_gain`.

## 17. Drawdown

Drawdown measures how far the portfolio has fallen from its historical peak up
to that date.

Formula:

```text
drawdown = (portfolio_value / running_peak) - 1
```

Where:

- `running_peak` is the maximum portfolio value observed so far

Interpretation:

- `0` means the portfolio is at a new high
- `-0.20` means the portfolio is 20% below its peak

This is stored in `portfolio_backtest_series.drawdown`.

## 18. Monte Carlo Portfolio Search

The optimized portfolio is selected using Monte Carlo simulation.

Current process:

1. generate random weight vectors
2. enforce practical constraints:
   - minimum active coins
   - maximum active coins
   - minimum active weight
3. compute each simulated portfolio's:
   - annualized return
   - annualized volatility
   - Sharpe ratio
   - diversification score
4. select the simulation with the highest Sharpe ratio

Important note:

- Monte Carlo does **not** test every possible portfolio
- it samples the feasible portfolio space randomly

## 19. Baseline, Benchmark, and Optimized Portfolios

### Baseline

The baseline portfolio is the user-defined current portfolio.

Current project setup:

```text
50% BTC + 50% ETH
```

### Benchmark

The benchmark is a simple reference portfolio.

Current project setup:

```text
100% BTC
```

### Optimized

The optimized portfolio is the best Monte Carlo portfolio under the current
constraints, chosen by maximum Sharpe ratio.

## 20. Summary of Main Metrics Used in Grafana

The dashboard currently relies on these key outputs:

- `sharpe_ratio`
- `avg_daily_return`
- `annualized_return`
- `portfolio_volatility`
- `annualized_volatility`
- `diversification_score`
- `diversification_label`
- `ending_value`
- `pct_gain`
- `correlation`
- `portfolio_value`
- `drawdown`

These are all derived from the aligned daily price history stored in
ClickHouse and computed in Task 6.
