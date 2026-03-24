SET allow_experimental_refreshable_materialized_view = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_technical_features_refresh
REFRESH EVERY 1 MINUTE
TO crypto.technical_features
AS
SELECT
    symbol,
    open_time,
    close,
    return_1m,
    ma_5,
    ma_15,
    volatility_15,
    now64(3) AS computed_at,
    toUnixTimestamp64Milli(now64(3)) AS version
FROM
(
    SELECT
        symbol,
        open_time,
        close,
        if(
            prev_close IS NULL OR prev_close = 0,
            NULL,
            (close_value - prev_close) / prev_close
        ) AS return_1m,
        avg(close_value) OVER (
            PARTITION BY symbol
            ORDER BY open_time
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma_5,
        avg(close_value) OVER (
            PARTITION BY symbol
            ORDER BY open_time
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS ma_15,
        stddevSamp(
            if(
                prev_close IS NULL OR prev_close = 0,
                NULL,
                (close_value - prev_close) / prev_close
            )
        ) OVER (
            PARTITION BY symbol
            ORDER BY open_time
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS volatility_15
    FROM
    (
        SELECT
            symbol,
            open_time,
            close,
            toFloat64(close) AS close_value,
            lagInFrame(toFloat64(close), 1) OVER (
                PARTITION BY symbol
                ORDER BY open_time
            ) AS prev_close
        FROM crypto.raw_ohlcv_1m
        WHERE is_closed = 1
    )
);
