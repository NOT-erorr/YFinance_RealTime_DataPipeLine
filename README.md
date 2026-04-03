# Yahoo Finance Data Pipeline

Real-time pipeline that ingests Yahoo Finance ticks, streams them through Kafka, and loads curated records into PostgreSQL and DuckDB.

## Architecture

```
Yahoo Finance WebSocket -> Kafka topic (stock-prices) -> ETL Loader -> PostgreSQL + DuckDB
```

## Normalized Structure

```
YF_datapipline/
├── producer/
│   └── fetch_data.py          # Ingestion service: Yahoo Finance -> Kafka
├── datapipeline/
│   ├── ETL.py                 # Processing service: Kafka -> PostgreSQL + DuckDB
│   └── settings.py            # Shared environment configuration
├── consumer/
│   └── consumer.py            # Compatibility entrypoint (delegates to ETL)
├── postgres/schemas/
│   └── init.sql               # PostgreSQL bootstrap schema (stock_prices)
├── duckdb/schemas/
│   └── init.sql               # DuckDB bootstrap schema (stock_prices)
├── data/
│   ├── raw/
│   └── processed/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Runtime Services

1. `kafka`: KRaft broker
2. `postgres`: persistent OLTP store
3. `producer`: publishes Yahoo Finance events to Kafka
4. `datapipeline`: consumes and writes normalized records to DBs

## Environment Variables

Main variables (see `.env.example`):

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `KAFKA_CONSUMER_GROUP`
- `YF_SYMBOLS`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `DUCKDB_PATH`
- `STOCK_TABLE`

## Run with Docker Compose

```bash
docker-compose up -d --build
```

To stop:

```bash
docker-compose down
```

## Current Notes

- Spark folders are preserved for future extension but not part of the active runtime flow.
- `consumer/consumer.py` is retained for backward compatibility and now delegates to `datapipeline/ETL.py`.
