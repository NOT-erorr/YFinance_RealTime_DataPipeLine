# Architecture

## Goal

Ingest market data from Yahoo Finance, stream through Kafka, then write in parallel to PostgreSQL and DuckDB.

## Runtime components

- producer: Fetches quotes and publishes messages to Kafka topic `stock-prices`.
- kafka: Single-node KRaft broker.
- consumer: Reads Kafka messages and writes to PostgreSQL and DuckDB in parallel worker queues.
- postgres: Persistent relational store.
- duckdb: Local analytics file store at `data/yf_analytics.duckdb`.
- grafana: Visualization entrypoint (port 3000).

## Data flow

1. Producer loads symbols from `/app/data/sp500_symbols.json`.
2. Producer fetches Yahoo quotes in batches.
3. If Yahoo returns no data, producer falls back to mock records.
4. Producer publishes each payload to Kafka topic `stock-prices`.
5. Consumer transforms message to normalized row format.
6. Consumer fans out row to:
   - PostgreSQL insert worker
   - DuckDB insert worker

## Message shape (normalized)

- symbol
- price
- timestamp
- datetime
- open
- high
- low
- close
- volume
- change
- change_percent
- currency
- source

## Tables

Both PostgreSQL and DuckDB use table name `stock_prices` with aligned schema fields above.
