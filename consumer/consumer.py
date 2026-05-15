import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import duckdb
import psycopg2
from aiokafka import AIOKafkaConsumer

from datapipeline.settings import (
    DUCKDB_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    STOCK_TABLE,
)


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("parallel_consumer")


class ParallelWarehouseConsumer:
    def __init__(self) -> None:
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.kafka_topic = KAFKA_TOPIC
        self.kafka_group_id = KAFKA_CONSUMER_GROUP

        self.postgres_host = POSTGRES_HOST
        self.postgres_port = POSTGRES_PORT
        self.postgres_user = POSTGRES_USER
        self.postgres_password = POSTGRES_PASSWORD
        self.postgres_db = POSTGRES_DB

        self.duckdb_path = DUCKDB_PATH
        self.stock_table = STOCK_TABLE

        self.consumer: Optional[AIOKafkaConsumer] = None

        self.pg_conn = None
        self.pg_cursor = None
        self.duck_conn = None

        self.pg_queue: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()
        self.duck_queue: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()

    def _ensure_db_connections(self) -> None:
        if self.pg_conn is None or self.pg_cursor is None or self.duck_conn is None:
            raise RuntimeError("Database connections are not initialized")

    def _build_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=self.kafka_group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def _init_tables(self) -> None:
        self._ensure_db_connections()
        pg_cursor = self.pg_cursor
        pg_conn = self.pg_conn
        duck_conn = self.duck_conn
        assert pg_cursor is not None and pg_conn is not None and duck_conn is not None

        pg_cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.stock_table} (
                symbol TEXT,
                price FLOAT,
                timestamp BIGINT,
                datetime TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                change FLOAT,
                change_percent FLOAT,
                currency TEXT,
                source TEXT,
                produced_at BIGINT,
                ingested_at TIMESTAMP
            )
            """
        )

        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS open FLOAT")
        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS high FLOAT")
        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS low FLOAT")
        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS close FLOAT")
        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS volume BIGINT")
        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS currency TEXT")
        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS source TEXT")
        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS produced_at BIGINT")
        pg_cursor.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP")
        pg_conn.commit()

        duck_conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.stock_table} (
                symbol VARCHAR,
                price DOUBLE,
                timestamp BIGINT,
                datetime TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                change DOUBLE,
                change_percent DOUBLE,
                currency VARCHAR,
                source VARCHAR,
                produced_at BIGINT,
                ingested_at TIMESTAMP
            )
            """
        )

        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS open DOUBLE")
        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS high DOUBLE")
        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS low DOUBLE")
        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS close DOUBLE")
        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS volume BIGINT")
        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS currency VARCHAR")
        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS source VARCHAR")
        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS produced_at BIGINT")
        duck_conn.execute(f"ALTER TABLE {self.stock_table} ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP")

    async def connect(self) -> None:
        self.consumer = self._build_consumer()
        await self.consumer.start()

        self.pg_conn = psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
        )
        self.pg_cursor = self.pg_conn.cursor()

        self.duck_conn = duckdb.connect(self.duckdb_path)
        self._init_tables()

        logger.info("Consumer connected to Kafka, PostgreSQL, and DuckDB")

    def transform(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            symbol = str(data["symbol"])
            close_price = float(data.get("close", data.get("price", 0.0)))
            open_price = float(data.get("open", close_price))
            high_price = float(data.get("high", close_price))
            low_price = float(data.get("low", close_price))
            volume = int(data.get("volume", 0))
            raw_ts = int(data["timestamp"])

            ts_ms = raw_ts if raw_ts > 10_000_000_000 else raw_ts * 1000
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(tzinfo=None)

            change_value = float(data.get("change", close_price - open_price))
            change_percent_value = float(
                data.get(
                    "change_percent",
                    (change_value / open_price * 100.0) if open_price else 0.0,
                )
            )

            raw_produced_at = data.get("produced_at")
            produced_at = int(raw_produced_at) if raw_produced_at else None
            ingested_at = datetime.utcnow()

            return {
                "symbol": symbol,
                "price": close_price,
                "timestamp": ts_ms,
                "datetime": dt,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "change": change_value,
                "change_percent": change_percent_value,
                "currency": str(data.get("currency", "USD")),
                "source": str(data.get("source", "unknown")),
                "produced_at": produced_at,
                "ingested_at": ingested_at,
            }
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Skip invalid payload %s, error=%s", data, exc)
            return None

    def _load_postgres_sync(self, row: Dict[str, Any]) -> None:
        self._ensure_db_connections()
        pg_cursor = self.pg_cursor
        pg_conn = self.pg_conn
        assert pg_cursor is not None and pg_conn is not None

        pg_cursor.execute(
            f"""
            INSERT INTO {self.stock_table}
            (symbol, price, timestamp, datetime, open, high, low, close, volume, change, change_percent, currency, source, produced_at, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["symbol"],
                row["price"],
                row["timestamp"],
                row["datetime"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["change"],
                row["change_percent"],
                row["currency"],
                row["source"],
                row["produced_at"],
                row["ingested_at"],
            ),
        )
        pg_conn.commit()

    def _load_duckdb_sync(self, row: Dict[str, Any]) -> None:
        self._ensure_db_connections()
        duck_conn = self.duck_conn
        assert duck_conn is not None

        duck_conn.execute(
            f"""
            INSERT INTO {self.stock_table}
            (symbol, price, timestamp, datetime, open, high, low, close, volume, change, change_percent, currency, source, produced_at, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["symbol"],
                row["price"],
                row["timestamp"],
                row["datetime"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["change"],
                row["change_percent"],
                row["currency"],
                row["source"],
                row["produced_at"],
                row["ingested_at"],
            ),
        )

    async def _postgres_worker(self) -> None:
        while True:
            row = await self.pg_queue.get()
            if row is None:
                self.pg_queue.task_done()
                break

            try:
                await asyncio.to_thread(self._load_postgres_sync, row)
            except Exception as exc:  # pylint: disable=broad-except
                if self.pg_conn is not None:
                    self.pg_conn.rollback()
                logger.exception("PostgreSQL worker failed: %s", exc)
            finally:
                self.pg_queue.task_done()

    async def _duckdb_worker(self) -> None:
        while True:
            row = await self.duck_queue.get()
            if row is None:
                self.duck_queue.task_done()
                break

            try:
                await asyncio.to_thread(self._load_duckdb_sync, row)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("DuckDB worker failed: %s", exc)
            finally:
                self.duck_queue.task_done()

    async def run(self) -> None:
        if self.consumer is None:
            await self.connect()
        if self.consumer is None:
            raise RuntimeError("Kafka consumer is not initialized")

        logger.info(
            "Start consumer loop topic=%s bootstrap=%s",
            self.kafka_topic,
            self.kafka_bootstrap_servers,
        )

        pg_task = asyncio.create_task(self._postgres_worker())
        duck_task = asyncio.create_task(self._duckdb_worker())

        try:
            async for message in self.consumer:
                raw = message.value
                logger.info("Received message: %s", raw)
                if not isinstance(raw, dict):
                    logger.warning("Skip non-dict payload: %s", raw)
                    continue

                row = self.transform(raw)
                if row is None:
                    continue

                # Fan-out to two independent warehouse flows in parallel.
                await asyncio.gather(self.pg_queue.put(row), self.duck_queue.put(row))
        finally:
            await asyncio.gather(self.pg_queue.put(None), self.duck_queue.put(None))
            await asyncio.gather(pg_task, duck_task)

    async def close(self) -> None:
        if self.consumer is not None:
            await self.consumer.stop()
        if self.pg_cursor is not None:
            self.pg_cursor.close()
        if self.pg_conn is not None:
            self.pg_conn.close()
        if self.duck_conn is not None:
            self.duck_conn.close()


async def main() -> None:
    app = ParallelWarehouseConsumer()
    try:
        await app.run()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        await app.close()


if __name__ == "__main__":
    asyncio.run(main())
