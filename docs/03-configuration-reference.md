# Configuration Reference

## Source of truth

Runtime config is consumed from environment variables in `datapipeline/settings.py` and service-level values in `docker-compose.yaml`.

## Kafka

- KAFKA_BOOTSTRAP_SERVERS (default: kafka:9092)
- KAFKA_TOPIC (default: stock-prices)
- KAFKA_CONSUMER_GROUP (default: stock-consumer-group)

## Producer

- YF_SYMBOLS_FILE: Path to symbols JSON (compose uses `/app/data/sp500_symbols.json`).
- YF_FETCH_INTERVAL_SECONDS: Poll interval (seconds).
- YF_BATCH_SIZE: Number of symbols per request batch.
- ENABLE_MOCK_FALLBACK: If true, writes mock data when Yahoo fetch is empty.

## PostgreSQL

- POSTGRES_HOST
- POSTGRES_PORT
- POSTGRES_USER
- POSTGRES_PASSWORD
- POSTGRES_DB

Default compose values:

- host: postgres
- port: 5432
- user: admin
- password: admin
- db: kraf_db

## DuckDB

- DUCKDB_PATH (compose: /app/data/yf_analytics.duckdb)
- STOCK_TABLE (default: stock_prices)

## Notes

- `.env.example` still contains `YF_SYMBOLS`, but current producer runtime uses `YF_SYMBOLS_FILE`.
- Keep `producer/sp500_symbols.json` and `data/sp500_symbols.json` synchronized if you change symbol universe.
