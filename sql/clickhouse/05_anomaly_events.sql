CREATE TABLE IF NOT EXISTS crypto.anomaly_events
(
    event_time DateTime64(3, 'UTC'),
    symbol LowCardinality(String),
    event_type LowCardinality(String),
    severity LowCardinality(String),
    observed_value Float64,
    expected_value Nullable(Float64),
    details String DEFAULT '',
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time, event_type);
