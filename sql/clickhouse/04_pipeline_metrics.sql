CREATE TABLE IF NOT EXISTS crypto.pipeline_metrics
(
    metric_time DateTime64(3, 'UTC') DEFAULT now64(3),
    component LowCardinality(String),
    metric_name LowCardinality(String),
    symbol Nullable(String),
    metric_value Float64,
    unit LowCardinality(String),
    job_name Nullable(String),
    run_id Nullable(String),
    details String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(metric_time)
ORDER BY (component, metric_name, metric_time, ifNull(symbol, ''));
