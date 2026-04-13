# Project Structure

## Current top-level layout

```text
YF_datapipline/
  config/
  consumer/
  data/
  datapipeline/
  docs/
  duckdb/
  grafana/
  logs/
  notebooks/
  postgres/
  producer/
  scripts/
  spark/
  tests/
  docker-compose.yaml
  Dockerfile
  requirements.txt
  README.md
```

## Key runtime files

- producer/fetch_data.py: Producer entrypoint and Yahoo/mock publishing loop.
- consumer/consumer.py: Kafka consumer with parallel fan-out to PostgreSQL and DuckDB.
- datapipeline/settings.py: Shared environment variable readers.
- docker-compose.yaml: Service orchestration.
- postgres/schemas/init.sql: PostgreSQL schema and compatibility grants.
- duckdb/schemas/init.sql: DuckDB schema.
- scripts/check_data.ps1: Health and data verification helper.

## Data artifacts

- data/yf_analytics.duckdb: DuckDB file generated at runtime.
- data/sp500_symbols.json: Runtime symbols file used by producer.

## Conventions

- Kafka topic: stock-prices
- Table name: stock_prices
- Primary DB credentials in compose: admin/admin
- Consumer writes to both warehouses in parallel
