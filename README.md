# Financial Data Pipeline: Kafka, Redshift, PySpark & PostgreSQL

## 📖 Giới thiệu
Dự án này xây dựng một Data Pipeline toàn diện để thu thập, lưu trữ và xử lý dữ liệu chứng khoán. Hệ thống sử dụng **Apache Kafka** để streaming dữ liệu từ **Yahoo Finance**, lưu trữ dữ liệu thô vào **AWS Redshift**, sau đó sử dụng **PySpark** để làm sạch và tính toán các chỉ số kỹ thuật trước khi lưu vào **PostgreSQL** để phục vụ phân tích hoặc hiển thị lên Dashboard.

## 🏗 Kiến trúc hệ thống
Luồng dữ liệu (Data Flow):
1.  **Source:** `yfinance` API (Python).
2.  **Message Queue:** Apache Kafka (Topic: `stock_market_data`).
3.  **Data Warehouse (Raw):** AWS Redshift (Lưu trữ dữ liệu thô nhận từ Kafka).
4.  **Processing:** PySpark (Đọc từ Redshift -> Xử lý/Transform -> Ghi ra Postgres).
5.  **Serving Database:** PostgreSQL (Lưu dữ liệu đã qua xử lý).

## 📂 Cấu trúc dự án
```text
project-root/
├── config/
│   ├── kafka_config.properties    # Cấu hình Kafka Producer/Consumer
│   └── database.ini               # Cấu hình kết nối Redshift/Postgres
├── src/
│   ├── producers/
│   │   └── stock_producer.py      # Script lấy data từ yfinance đẩy vào Kafka
│   ├── consumers/
│   │   └── redshift_sink.py       # (Hoặc config Kafka Connect) Đẩy data vào Redshift
│   ├── spark_jobs/
│   │   └── transform_job.py       # PySpark ETL logic
│   └── utils/
│       └── db_connector.py        # Helper connect DB
├── docker/
│   ├── docker-compose.yaml        # Kafka, Zookeeper, Postgres (Local dev)
│   └── Dockerfile                 # Image cho Spark/Producer
├── requirements.txt
└── README.md