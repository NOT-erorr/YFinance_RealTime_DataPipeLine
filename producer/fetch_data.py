import asyncio
import json
import logging
import os
import random
import time
from typing import Optional

import yfinance as yf
from aiokafka import AIOKafkaProducer

from datapipeline.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, YF_SYMBOLS

logging.basicConfig(level=logging.INFO)

SYMBOLS = YF_SYMBOLS
TOPIC = KAFKA_TOPIC
FETCH_INTERVAL_SECONDS = int(os.getenv("YF_FETCH_INTERVAL_SECONDS", "15"))
ENABLE_MOCK_FALLBACK = os.getenv("ENABLE_MOCK_FALLBACK", "true").lower() == "true"

producer: Optional[AIOKafkaProducer] = None


async def init_kafka():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    logging.info("Kafka producer started")


def transform(msg):
    return {
        "symbol": msg.get("id"),
        "price": msg.get("price"),
        "timestamp": int(msg.get("time", 0))
    }


def fetch_latest_quotes():
    rows = []
    for symbol in SYMBOLS:
        try:
            history = yf.Ticker(symbol).history(period="1d", interval="1m")
            if history.empty:
                continue

            last_row = history.iloc[-1]
            ts_ms = int(history.index[-1].timestamp() * 1000)
            rows.append(
                {
                    "symbol": symbol,
                    "price": float(last_row["Close"]),
                    "timestamp": ts_ms,
                }
            )
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("Fetch error for %s: %s", symbol, exc)

    if not rows and ENABLE_MOCK_FALLBACK:
        now_ms = int(time.time() * 1000)
        for symbol in SYMBOLS:
            rows.append(
                {
                    "symbol": symbol,
                    "price": round(random.uniform(150.0, 350.0), 2),
                    "timestamp": now_ms,
                }
            )
        logging.warning("Yahoo data unavailable, using mock fallback batch")

    return rows


async def fetch_and_stream():
    if producer is None:
        raise RuntimeError("Kafka producer is not initialized")
    active_producer = producer

    logging.info("Start polling Yahoo Finance symbols=%s every %ss", SYMBOLS, FETCH_INTERVAL_SECONDS)
    while True:
        rows = await asyncio.to_thread(fetch_latest_quotes)
        for data in rows:
            await active_producer.send(TOPIC, data)
            logging.info("Sent to Kafka: %s", data)

        await asyncio.sleep(FETCH_INTERVAL_SECONDS)


async def main():
    await init_kafka()

    try:
        await fetch_and_stream()
    finally:
        if producer is not None:
            await producer.stop()
            logging.info("Kafka producer stopped")


if __name__ == "__main__":
    asyncio.run(main())