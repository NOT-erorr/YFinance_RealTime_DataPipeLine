<<<<<<< HEAD
# 📈 Module Producer: Yahoo Finance Real-Time Data Streamer

Module này đóng vai trò là **Producer** trong hệ thống Data Pipeline. Nhiệm vụ chính là thu thập dữ liệu giá cổ phiếu thời gian thực từ Yahoo Finance và đẩy vào **Kafka topic** để các module khác (như Spark hoặc DuckDB) xử lý.

## 🚀 Tính năng chính
* **Tự động hóa:** Lấy dữ liệu giá S&P 500 theo chu kỳ (mặc định 60 giây).
* **Xử lý bất đồng bộ (Async):** Sử dụng `aiokafka` để gửi dữ liệu hiệu suất cao, không gây nghẽn luồng.
* **Xử lý lỗi thông minh (Mock Fallback):** Nếu Yahoo Finance gặp lỗi hoặc không trả về dữ liệu, hệ thống tự động sinh dữ liệu giả lập (mock data) để đảm bảo pipeline không bị ngắt quãng.
* **Tối ưu hóa Batch:** Chia nhỏ danh sách cổ phiếu thành các batch (mặc định 50 mã) để tránh bị API giới hạn.

## 🛠 Yêu cầu hệ thống
Để chạy được module này, bạn cần cài đặt các thư viện Python sau:

```bash
pip install yfinance aiokafka asyncio
=======
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
>>>>>>> 99219cf313ec97a0af59753d027c3d0899504f03
