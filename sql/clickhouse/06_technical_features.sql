CREATE TABLE IF NOT EXISTS crypto.technical_features
(
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    close Decimal(18, 8),
    return_1m Nullable(Float64),
    ma_5 Nullable(Float64),
    ma_15 Nullable(Float64),
    volatility_15 Nullable(Float64),
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    version UInt64 DEFAULT toUnixTimestamp64Milli(computed_at)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time);
