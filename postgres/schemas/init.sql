CREATE TABLE IF NOT EXISTS stock_prices (
    symbol TEXT,
    price DOUBLE PRECISION,
    timestamp BIGINT,
    datetime TIMESTAMP,
    change DOUBLE PRECISION,
    change_percent DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_datetime ON stock_prices(symbol, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_stock_prices_datetime ON stock_prices(datetime DESC);
