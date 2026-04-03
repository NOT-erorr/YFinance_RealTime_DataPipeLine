CREATE TABLE IF NOT EXISTS stock_prices (
    symbol VARCHAR,
    price DOUBLE,
    timestamp BIGINT,
    datetime TIMESTAMP,
    change DOUBLE,
    change_percent DOUBLE
);
