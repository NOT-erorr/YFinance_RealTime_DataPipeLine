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
logger = logging.getLogger("finance_etl")


class FinanceETL:
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

	def _ensure_connections(self) -> None:
		if self.pg_conn is None or self.pg_cursor is None or self.duck_conn is None:
			raise RuntimeError("Database connections are not initialized")

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

		logger.info("ETL connected to Kafka, PostgreSQL, and DuckDB")

	def _build_consumer(self) -> AIOKafkaConsumer:
		return AIOKafkaConsumer(
			self.kafka_topic,
			bootstrap_servers=self.kafka_bootstrap_servers,
			group_id=self.kafka_group_id,
			auto_offset_reset="earliest",
			value_deserializer=lambda v: json.loads(v.decode("utf-8")),
		)

	def _init_tables(self) -> None:
		self._ensure_connections()
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
				change FLOAT,
				change_percent FLOAT
			)
			"""
		)
		pg_conn.commit()

		duck_conn.execute(
			f"""
			CREATE TABLE IF NOT EXISTS {self.stock_table} (
				symbol VARCHAR,
				price DOUBLE,
				timestamp BIGINT,
				datetime TIMESTAMP,
				change DOUBLE,
				change_percent DOUBLE
			)
			"""
		)

	def transform(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
		try:
			symbol = str(data["symbol"])
			price = float(data["price"])
			raw_ts = int(data["timestamp"])

			# Accept seconds or milliseconds timestamps.
			ts_ms = raw_ts if raw_ts > 10_000_000_000 else raw_ts * 1000
			dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(tzinfo=None)

			return {
				"symbol": symbol,
				"price": price,
				"timestamp": ts_ms,
				"datetime": dt,
				"change": float(data.get("change", 0.0)),
				"change_percent": float(data.get("change_percent", 0.0)),
			}
		except (KeyError, TypeError, ValueError) as exc:
			logger.warning("Skip invalid payload %s, error=%s", data, exc)
			return None

	def load_postgres(self, row: Dict[str, Any]) -> None:
		self._ensure_connections()
		pg_cursor = self.pg_cursor
		pg_conn = self.pg_conn
		assert pg_cursor is not None and pg_conn is not None

		pg_cursor.execute(
			f"""
			INSERT INTO {self.stock_table}
			(symbol, price, timestamp, datetime, change, change_percent)
			VALUES (%s, %s, %s, %s, %s, %s)
			""",
			(
				row["symbol"],
				row["price"],
				row["timestamp"],
				row["datetime"],
				row["change"],
				row["change_percent"],
			),
		)
		pg_conn.commit()

	def load_duckdb(self, row: Dict[str, Any]) -> None:
		self._ensure_connections()
		duck_conn = self.duck_conn
		assert duck_conn is not None

		duck_conn.execute(
			f"""
			INSERT INTO {self.stock_table}
			(symbol, price, timestamp, datetime, change, change_percent)
			VALUES (?, ?, ?, ?, ?, ?)
			""",
			(
				row["symbol"],
				row["price"],
				row["timestamp"],
				row["datetime"],
				row["change"],
				row["change_percent"],
			),
		)

	async def run(self) -> None:
		if self.consumer is None:
			await self.connect()
		if self.consumer is None:
			raise RuntimeError("Kafka consumer is not initialized")

		logger.info(
			"Start ETL consume loop topic=%s bootstrap=%s",
			self.kafka_topic,
			self.kafka_bootstrap_servers,
		)

		async for message in self.consumer:
			raw = message.value
			logger.info("Received message: %s", raw)
			if not isinstance(raw, dict):
				logger.warning("Skip non-dict payload: %s", raw)
				continue

			row = self.transform(raw)
			if row is None:
				continue

			try:
				self.load_postgres(row)
				self.load_duckdb(row)
			except Exception as exc:  # pylint: disable=broad-except
				if self.pg_conn is not None:
					self.pg_conn.rollback()
				logger.exception("Failed to load row: %s", exc)

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
	etl = FinanceETL()
	try:
		await etl.run()
	except KeyboardInterrupt:
		logger.info("ETL stopped by user")
	finally:
		await etl.close()


if __name__ == "__main__":
	asyncio.run(main())
