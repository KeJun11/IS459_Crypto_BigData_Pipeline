CREATE TABLE IF NOT EXISTS crypto.agg_ohlcv_1h
(
    symbol LowCardinality(String),
    bucket_start DateTime64(3, 'UTC'),
    open Decimal(18, 8),
    high Decimal(18, 8),
    low Decimal(18, 8),
    close Decimal(18, 8),
    volume Decimal(18, 8),
    source LowCardinality(String),
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    version UInt64 DEFAULT toUnixTimestamp64Milli(computed_at)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (symbol, bucket_start);
