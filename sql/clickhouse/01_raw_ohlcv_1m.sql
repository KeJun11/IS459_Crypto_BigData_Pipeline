CREATE TABLE IF NOT EXISTS crypto.raw_ohlcv_1m
(
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open Decimal(18, 8),
    high Decimal(18, 8),
    low Decimal(18, 8),
    close Decimal(18, 8),
    volume Decimal(18, 8),
    is_closed UInt8,
    source LowCardinality(String),
    ingest_id UUID DEFAULT generateUUIDv4(),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    version UInt64 DEFAULT toUnixTimestamp64Milli(ingested_at)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time);
