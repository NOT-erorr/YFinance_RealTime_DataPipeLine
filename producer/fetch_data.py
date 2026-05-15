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


BATCH_SIZE = int(os.getenv("YF_BATCH_SIZE", "50"))

_YF_LOGGER = logging.getLogger("yfinance")
_URLLIB3_LOGGER = logging.getLogger("urllib3")


def fetch_batch(batch_symbols: list) -> list:
    # Suppress per-ticker noise from yfinance internals; we log at batch level.
    prev_yf = _YF_LOGGER.level
    prev_u3 = _URLLIB3_LOGGER.level
    _YF_LOGGER.setLevel(logging.CRITICAL)
    _URLLIB3_LOGGER.setLevel(logging.CRITICAL)
    try:
        raw = yf.download(
            batch_symbols,
            period="1d",
            interval="1m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Batch download failed (%d symbols): %s", len(batch_symbols), exc)
        return []
    finally:
        _YF_LOGGER.setLevel(prev_yf)
        _URLLIB3_LOGGER.setLevel(prev_u3)

    if raw is None or raw.empty:
        logging.debug("Batch returned empty (%d symbols)", len(batch_symbols))
        return []

    rows = []
    single = len(batch_symbols) == 1
    for symbol in batch_symbols:
        try:
            hist = raw if single else raw[symbol]
            if hist.empty:
                continue
            last = hist.iloc[-1]
            ts_ms = int(hist.index[-1].timestamp() * 1000)
            rows.append({
                "symbol": symbol,
                "price": float(last["Close"]),
                "timestamp": ts_ms,
                "open": float(last["Open"]),
                "high": float(last["High"]),
                "low": float(last["Low"]),
                "close": float(last["Close"]),
                "volume": int(last["Volume"]),
                "source": "yfinance",
            })
        except Exception:  # pylint: disable=broad-except
            pass
    return rows


def fetch_latest_quotes():
    rows = []
    for i in range(0, len(SYMBOLS), BATCH_SIZE):
        batch = SYMBOLS[i:i + BATCH_SIZE]
        rows.extend(fetch_batch(batch))

    if rows:
        logging.info("Fetched %d real quotes from Yahoo Finance", len(rows))
    elif ENABLE_MOCK_FALLBACK:
        now_ms = int(time.time() * 1000)
        for symbol in SYMBOLS:
            close = round(random.uniform(50.0, 500.0), 2)
            change_pct = round(random.uniform(-4.0, 4.0), 2)
            open_ = round(close / (1 + change_pct / 100), 2)
            spread = close * random.uniform(0.005, 0.02)
            rows.append({
                "symbol": symbol,
                "price": close,
                "timestamp": now_ms,
                "open": open_,
                "high": round(max(close, open_) + spread, 2),
                "low": round(min(close, open_) - spread, 2),
                "close": close,
                "volume": random.randint(500_000, 20_000_000),
                "change": round(close - open_, 2),
                "change_percent": change_pct,
                "source": "mock",
            })
        logging.warning("Yahoo unavailable — mock fallback for %d symbols", len(rows))

    return rows


async def fetch_and_stream():
    if producer is None:
        raise RuntimeError("Kafka producer is not initialized")
    active_producer = producer

    logging.info("Start polling Yahoo Finance symbols=%s every %ss", SYMBOLS, FETCH_INTERVAL_SECONDS)
    while True:
        rows = await asyncio.to_thread(fetch_latest_quotes)
        for data in rows:
            data["produced_at"] = int(time.time() * 1000)
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