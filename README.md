# Yahoo Finance Data Pipeline

Real-time pipeline that ingests Yahoo Finance ticks, streams them through Kafka, and writes records in parallel into PostgreSQL and DuckDB.

## Architecture

```
Yahoo Finance Polling -> Kafka topic (stock-prices) -> Consumer (parallel fan-out) -> PostgreSQL + DuckDB
```

## Normalized Structure

```
YF_datapipline/
├── producer/
│   └── fetch_data.py          # Ingestion service: Yahoo Finance -> Kafka
├── datapipeline/
│   └── settings.py            # Shared environment configuration
├── consumer/
│   └── consumer.py            # Main consumer: Kafka -> PostgreSQL + DuckDB (parallel workers)
├── postgres/schemas/
│   └── init.sql               # PostgreSQL bootstrap schema (stock_prices)
├── duckdb/schemas/
│   └── init.sql               # DuckDB bootstrap schema (stock_prices)
├── data/
│   ├── raw/
│   └── processed/
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── .env.example
```

## Runtime Services

1. `kafka`: KRaft broker
2. `postgres`: persistent OLTP store
3. `producer`: publishes Yahoo Finance events to Kafka
4. `consumer`: consumes and writes to both warehouses in parallel

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
docker compose -f docker-compose.yaml up -d --build
```

To stop:

```bash
docker compose -f docker-compose.yaml down
```

## Current Notes

- Spark folders are preserved for future extension but not part of the active runtime flow.
- `producer/fetch_data.py` includes a mock fallback mode when Yahoo endpoint data is unavailable.
