# Big Data Crypto Analytics Pipeline -- Project Roadmap

## Project Goal

Build a **serverless big data pipeline** that ingests cryptocurrency
market and trending data, stores historical snapshots, transforms the
data into analytics-ready datasets, and visualizes insights in an
interactive dashboard.

The pipeline will focus on **Top 50 cryptocurrencies and trending
coins**, enabling analysis of: - Market trends - Price movements -
Volume changes - Trending frequency - Rank changes

------------------------------------------------------------------------

# 1. High-Level Architecture

Source → Ingestion → Storage → Transformation → Analytics → Dashboard

**Proposed AWS Architecture**

CoinGecko API\
↓\
EventBridge Scheduler (trigger every 10 minutes or hourly)\
↓\
AWS Lambda (Python ingestion script)\
↓\
Amazon S3 (Data Lake)\
↓\
AWS Glue Data Catalog\
↓\
Amazon Athena (SQL transformations)\
↓\
Amazon QuickSight (Dashboard)

------------------------------------------------------------------------

# 2. Technology Stack

## Ingestion Layer

Tools: - Python - AWS Lambda - EventBridge Scheduler

Responsibilities: - Pull crypto data from CoinGecko API - Store raw
responses in S3 - Run ingestion on a schedule

API Endpoints Example: - Trending coins - Top 50 coins by market cap -
Market metrics (price, volume, market cap)

------------------------------------------------------------------------

# 3. Data Lake Storage Design

The project will use a **data lake architecture with layered datasets**.

### Bronze Layer (Raw Data)

Stores untouched API responses.

Example structure:

s3://crypto-project/raw/trending/YYYY/MM/DD/HH/trending.json\
s3://crypto-project/raw/markets/YYYY/MM/DD/HH/markets.json

Format: JSON

Purpose: - Historical snapshots - Data reproducibility - Debugging

------------------------------------------------------------------------

### Silver Layer (Cleaned Data)

Cleaned and structured datasets.

Transformations: - Flatten JSON - Convert timestamps - Remove
duplicates - Standardize column names

Format: Parquet

Example path:

s3://crypto-project/silver/coin_snapshots/

------------------------------------------------------------------------

### Gold Layer (Analytics Tables)

Optimized for dashboards and analysis.

Example datasets: - Daily coin metrics - Trending frequency - Market
volatility indicators

Example path:

s3://crypto-project/gold/daily_coin_metrics/

------------------------------------------------------------------------

# 4. Data Modeling

## Dimension Table

### dim_coin

Columns: - coin_id - symbol - name - category - platform -
first_seen_date

Purpose: Stores metadata about cryptocurrencies.

------------------------------------------------------------------------

## Fact Tables

### fact_coin_snapshots

Columns: - snapshot_time - coin_id - current_price - market_cap -
total_volume - price_change_24h - market_cap_rank

Purpose: Time-series market metrics for each coin.

------------------------------------------------------------------------

### fact_trending_snapshots

Columns: - snapshot_time - coin_id - trending_rank - source

Purpose: Tracks which coins appear in trending lists.

------------------------------------------------------------------------

### agg_daily_coin_metrics

Columns: - date - coin_id - avg_price - max_price - min_price -
avg_volume - times_trending - rank_change - volatility_score

Purpose: Aggregated analytics for dashboards.

------------------------------------------------------------------------

# 5. Data Transformation Layer

Primary Tool: Amazon Athena

Responsibilities: - Query S3 datasets - Create analytical tables -
Compute metrics

Example transformations:

Compute daily averages\
Detect price volatility\
Calculate trending frequency

Optional tools: - AWS Glue ETL - dbt for incremental models

------------------------------------------------------------------------

# 6. Dashboard Design

Visualization Tool: Amazon QuickSight

The dashboard will contain three sections.

## Market Overview

Visuals: - Total market cap of tracked assets - Average 24h price
change - Top gainers and losers - Volume leaders

Charts: - Leaderboards - Bar charts - KPI cards

------------------------------------------------------------------------

## Trending Analysis

Insights: - Most frequently trending coins - Trending frequency over
time - Trending vs market cap comparison

Charts: - Line charts - Heatmaps - Trend rankings

------------------------------------------------------------------------

## Coin Deep Dive

User can select a specific coin.

Visuals: - Price over time - Volume over time - Market cap trend -
Trending appearances

Charts: - Time-series line charts - Scatter plots - Historical
comparison charts

------------------------------------------------------------------------

# 7. Pipeline Scheduling

EventBridge Scheduler triggers ingestion.

Suggested schedule:

Trending endpoint: every 10 minutes\
Market data snapshot: every 10 minutes or hourly

Benefits: - Creates historical time-series data - Enables trend analysis

------------------------------------------------------------------------

# 8. Monitoring and Logging

Use AWS CloudWatch.

Track: - Lambda execution logs - API request failures - Data ingestion
counts

Basic data quality checks: - No null coin_id - No duplicate (coin_id,
timestamp) - Expected row counts

------------------------------------------------------------------------

# 9. Implementation Milestones

## Milestone 1 -- Data Ingestion

-   Connect to CoinGecko API
-   Write ingestion script
-   Store raw data in S3

## Milestone 2 -- Data Catalog

-   Register datasets with AWS Glue
-   Query data using Athena

## Milestone 3 -- Data Transformation

-   Build cleaned silver tables
-   Create gold analytics tables

## Milestone 4 -- Dashboard

-   Connect Athena to QuickSight
-   Build dashboard visualizations

## Milestone 5 -- Enhancements

Optional features: - Trend frequency scoring - Volatility detection -
Alerting on unusual volume spikes

------------------------------------------------------------------------

# 10. Future Improvements

Possible extensions: - Real-time streaming with Kinesis - ML predictions
for price movement - Sentiment analysis from crypto news - Additional
data sources

------------------------------------------------------------------------

# Final Outcome

A fully functional **serverless big data pipeline** capable of:

-   Ingesting cryptocurrency market data
-   Maintaining historical datasets
-   Transforming raw data into analytics tables
-   Delivering actionable insights through dashboards
