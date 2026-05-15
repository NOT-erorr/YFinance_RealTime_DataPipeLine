# Yahoo Finance Real-Time Data Pipeline

A containerized, end-to-end streaming pipeline that ingests live stock quotes from Yahoo Finance, routes them through Apache Kafka, and writes in parallel to two complementary data stores — with a Grafana dashboard for real-time visualization.

---

## Architecture

```
┌───────────────────────┐
│    Yahoo Finance API  │
│   (S&P 500 symbols)   │
└──────────┬────────────┘
           │  polling every 60s (async batches of 50)
           ▼
┌───────────────────────┐
│       Producer        │  ← mock fallback when API is unavailable
│   producer/fetch_data │
└──────────┬────────────┘
           │  JSON messages
           ▼
┌───────────────────────┐
│   Apache Kafka        │
│  topic: stock-prices  │  KRaft mode (no ZooKeeper)
└──────────┬────────────┘
           │  consumer group
           ▼
┌───────────────────────┐
│  ParallelWarehouse    │
│      Consumer         │  ← two independent async worker queues
└────────┬──────┬───────┘
         │      │  fan-out
         ▼      ▼
┌──────────┐  ┌──────────────────────┐
│PostgreSQL│  │       DuckDB         │
│  OLTP    │  │  OLAP / analytics    │
│ :5432    │  │  yf_analytics.duckdb │
└────┬─────┘  └──────────────────────┘
     │
     ▼
┌───────────────────────┐
│       Grafana         │
│  Stock Pipeline       │
│  Monitor  :3000       │
└───────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Ingestion | Python (asyncio + yfinance) | 3.12 |
| Messaging | Apache Kafka (KRaft) | 7.6.1 |
| Async Kafka client | aiokafka | 0.10.0 |
| OLTP warehouse | PostgreSQL | 15 |
| OLAP warehouse | DuckDB | 0.10.0 |
| Visualization | Grafana Enterprise | latest |
| Containerization | Docker Compose | V2 |

---

## Project Structure

```
YF_datapipline/
│
├── producer/
│   └── fetch_data.py          # Yahoo Finance → Kafka (async, batched)
│
├── consumer/
│   └── consumer.py            # Kafka → PostgreSQL + DuckDB (parallel workers)
│
├── datapipeline/
│   └── settings.py            # Shared environment config loader
│
├── postgres/
│   └── schemas/init.sql       # PostgreSQL bootstrap schema
│
├── duckdb/
│   └── schemas/init.sql       # DuckDB bootstrap schema
│
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── postgres.yaml  # Auto-configured PostgreSQL datasource
│       └── dashboards/
│           ├── provider.yaml  # Dashboard file provider config
│           └── stock_monitor.json  # Pre-built monitoring dashboard
│
├── data/
│   ├── sp500_symbols.json     # S&P 500 ticker list (~500 symbols)
│   ├── raw/
│   └── processed/
│
├── scripts/
│   └── check_data.ps1         # PowerShell utility for quick data validation
│
├── docs/                      # Extended documentation
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── .env.example
```

---

## Services

| Container | Image | Port | Role |
|---|---|---|---|
| `kafka-kraft` | confluentinc/cp-kafka:7.6.1 | 9092, 9093 | KRaft broker (no ZooKeeper) |
| `postgres-kraf` | postgres:15 | 5432 | OLTP persistent store |
| `producer-kraf` | custom | — | Fetches Yahoo quotes → publishes to Kafka |
| `consumer-kraf` | custom | — | Consumes Kafka → writes to both warehouses |
| `grafana` | grafana/grafana-enterprise | 3000 | Dashboards and monitoring |

---

## Data Schema

Both PostgreSQL and DuckDB use table `stock_prices` with the same 13 columns:

| Column | Type | Description |
|---|---|---|
| `symbol` | TEXT | Ticker symbol (e.g. `AAPL`, `MSFT`) |
| `price` | FLOAT | Latest close price |
| `timestamp` | BIGINT | Unix timestamp in milliseconds |
| `datetime` | TIMESTAMP | UTC datetime of the quote |
| `open` | FLOAT | Opening price |
| `high` | FLOAT | Intraday high |
| `low` | FLOAT | Intraday low |
| `close` | FLOAT | Closing price (same as `price`) |
| `volume` | BIGINT | Trade volume |
| `change` | FLOAT | Absolute price change |
| `change_percent` | FLOAT | Percentage change |
| `currency` | TEXT | Currency code (default: `USD`) |
| `source` | TEXT | Data source (`yfinance` or `mock`) |

PostgreSQL indexes: `(symbol, datetime DESC)` and `(datetime DESC)`.

---

## Quick Start

**Prerequisites:** Docker Desktop with Compose V2. Ports 3000, 5432, 9092, 9093 must be free.

```bash
# 1. Clone and enter the directory
cd YF_datapipline

# 2. Start the full stack
docker compose up -d --build

# 3. Check all services are running
docker compose ps
```

Expected output — all services should show `running` or `healthy`:

```
NAME             STATUS
kafka-kraft      running (healthy)
postgres-kraf    running (healthy)
producer-kraf    running
consumer-kraf    running
grafana          running
```

```bash
# 4. Open Grafana
# http://localhost:3000  →  admin / admin
```

---

## Grafana Dashboard

The **Stock Pipeline Monitor** dashboard is provisioned automatically on startup — no manual setup required.

**URL:** http://localhost:3000 · **Login:** `admin` / `admin`

| Panel | Type | Description |
|---|---|---|
| Total Records | Stat | Cumulative rows in PostgreSQL |
| Records (Last 5 min) | Stat | Recent ingestion health indicator |
| Active Symbols (5 min) | Stat | Number of symbols actively streaming |
| Last Ingestion | Stat | Timestamp of the most recent write |
| Stock Price Trend | Time series | Multi-symbol price chart with interval grouping |
| Latest Prices Snapshot | Table | Per-symbol snapshot with OHLCV + change% |
| Top Movers — Change % | Table | 15 symbols with largest absolute price movement |
| Ingestion Rate | Time series | Pipeline throughput in rows/minute |

The **Symbol** variable at the top lets you filter the price trend chart to any combination of symbols. Auto-refreshes every 30 seconds.

---

## Configuration

All runtime configuration is read from environment variables. The defaults below are set directly in `docker-compose.yaml`.

### Kafka

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Broker address |
| `KAFKA_TOPIC` | `stock-prices` | Topic name |
| `KAFKA_CONSUMER_GROUP` | `stock-consumer-group` | Consumer group ID |

### Producer

| Variable | Default | Description |
|---|---|---|
| `YF_SYMBOLS_FILE` | `/app/data/sp500_symbols.json` | Path to ticker list |
| `YF_FETCH_INTERVAL_SECONDS` | `60` | Poll interval in seconds |
| `YF_BATCH_SIZE` | `50` | Symbols per API request batch |
| `ENABLE_MOCK_FALLBACK` | `true` | Generate synthetic data if Yahoo returns empty |

### PostgreSQL

| Variable | Default |
|---|---|
| `POSTGRES_HOST` | `postgres` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_USER` | `admin` |
| `POSTGRES_PASSWORD` | `admin` |
| `POSTGRES_DB` | `kraf_db` |

### DuckDB

| Variable | Default | Description |
|---|---|---|
| `DUCKDB_PATH` | `/app/data/yf_analytics.duckdb` | File path inside container |
| `STOCK_TABLE` | `stock_prices` | Table name in both warehouses |

---

## Validate Ingestion

```bash
# Row count in PostgreSQL
docker compose exec -T postgres \
  psql -U admin -d kraf_db -c "SELECT COUNT(*) FROM stock_prices;"

# Latest 10 rows
docker compose exec -T postgres \
  psql -U admin -d kraf_db \
  -c "SELECT symbol, price, source, datetime FROM stock_prices ORDER BY datetime DESC LIMIT 10;"

# Rows per minute over the last hour
docker compose exec -T postgres \
  psql -U admin -d kraf_db \
  -c "SELECT DATE_TRUNC('minute', datetime) AS minute, COUNT(*) FROM stock_prices WHERE datetime >= NOW() - INTERVAL '1 hour' GROUP BY 1 ORDER BY 1 DESC;"
```

PowerShell helper script (Windows):

```powershell
./scripts/check_data.ps1 -ComposeFile docker-compose.yaml -ShowSampleRows
```

---

## Useful Operations

```bash
# Tail live logs from producer and consumer
docker compose logs -f producer consumer

# Restart only the producer (e.g. after symbol file change)
docker compose up -d --force-recreate producer

# Restart only the consumer
docker compose up -d --force-recreate consumer

# Full teardown (data volumes are preserved)
docker compose down

# Full teardown including all volumes (wipes stored data)
docker compose down -v
```

---

## Troubleshooting

### No data in PostgreSQL

```bash
docker compose exec -T postgres \
  psql -U admin -d kraf_db \
  -c "SELECT NOW(), MAX(datetime), COUNT(*) FROM stock_prices;"
```

- Confirm the database is `kraf_db` and table is `stock_prices`.
- If `MAX(datetime)` is far in the past, the consumer may have stopped — check `docker compose logs consumer`.

### Producer logs show Yahoo fetch errors

This is expected when Yahoo Finance rate-limits or returns empty data. The producer automatically switches to **mock fallback** (randomized prices, `source = 'mock'`), keeping the pipeline running. Reduce `YF_BATCH_SIZE` or lower `YF_FETCH_INTERVAL_SECONDS` if real data is required.

### Grafana shows "No data"

1. Verify PostgreSQL is healthy: `docker compose ps`.
2. Check that at least some rows exist in `stock_prices`.
3. Widen the Grafana time range (top-right) — default is Last 1 hour.

### DuckDB locked

The DuckDB file is held by the consumer while running. Use the `duckdb-check` utility container (tools profile) for read-only inspection, or query PostgreSQL directly via Grafana.

---

## Notes

- **Spark** folders are preserved for future extension but are not part of the active runtime.
- **Mock fallback** is enabled by default so the pipeline does not stop when Yahoo Finance is unavailable or rate-limited.
- `data/sp500_symbols.json` and the compose volume `./data` are shared between producer, consumer, and Grafana containers.
