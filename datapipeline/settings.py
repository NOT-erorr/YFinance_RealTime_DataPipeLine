import os
from typing import List


def csv_to_list(raw_value: str, fallback: List[str]) -> List[str]:
    values = [item.strip() for item in raw_value.split(",") if item.strip()]
    return values or fallback


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "stock-consumer-group")

YF_SYMBOLS = csv_to_list(os.getenv("YF_SYMBOLS", "AAPL,MSFT,GOOG"), ["AAPL", "MSFT", "GOOG"])

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
POSTGRES_DB = os.getenv("POSTGRES_DB", "kraf_db")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/yf_analytics.duckdb")
STOCK_TABLE = os.getenv("STOCK_TABLE", "stock_prices")
