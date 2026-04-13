"""
fetch_data.py
-------------
Poll giá cổ phiếu S&P 500 từ Yahoo Finance và stream vào Kafka.
Symbols được load từ file sp500_symbols.json (hardcoded, không fetch network).

Env vars:
    YF_SYMBOLS_FILE            Path tới file JSON (default: sp500_symbols.json)
    YF_FETCH_INTERVAL_SECONDS  Chu kỳ poll tính bằng giây (default: 60)
    YF_BATCH_SIZE              Số symbols mỗi batch (default: 50)
    ENABLE_MOCK_FALLBACK       Dùng dữ liệu giả nếu Yahoo trả về rỗng (default: true)
"""

import asyncio
import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Optional

import yfinance as yf
from aiokafka import AIOKafkaProducer

from datapipeline.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOLS_FILE = Path(os.getenv("YF_SYMBOLS_FILE", "sp500_symbols.json"))
TOPIC = KAFKA_TOPIC
FETCH_INTERVAL_SECONDS = int(os.getenv("YF_FETCH_INTERVAL_SECONDS", "60"))
BATCH_SIZE = int(os.getenv("YF_BATCH_SIZE", "50"))
ENABLE_MOCK_FALLBACK = os.getenv("ENABLE_MOCK_FALLBACK", "true").lower() == "true"
DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"]

# ── Producer global ───────────────────────────────────────────────────────────
producer: Optional[AIOKafkaProducer] = None


# ── Symbol loading ────────────────────────────────────────────────────────────

def load_symbols() -> list[str]:
    """Load symbols từ file JSON; fallback sang danh sách mặc định nếu file thiếu."""
    if not SYMBOLS_FILE.exists():
        logging.warning(
            "Symbols file not found: '%s'. Falling back to default symbols (%d).",
            SYMBOLS_FILE,
            len(DEFAULT_SYMBOLS),
        )
        return DEFAULT_SYMBOLS
    with open(SYMBOLS_FILE, encoding="utf-8") as f:
        symbols: list[str] = json.load(f)

    if not symbols:
        logging.warning("Symbols file is empty. Falling back to default symbols (%d).", len(DEFAULT_SYMBOLS))
        return DEFAULT_SYMBOLS

    logging.info("Loaded %d symbols from %s", len(symbols), SYMBOLS_FILE)
    return symbols


SYMBOLS: list[str] = load_symbols()


# ── Kafka ─────────────────────────────────────────────────────────────────────

async def init_kafka() -> None:
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    logging.info("Kafka producer started (bootstrap=%s, topic=%s)", KAFKA_BOOTSTRAP_SERVERS, TOPIC)


# ── Fetch helpers ─────────────────────────────────────────────────────────────

def _make_mock_rows(symbols: list[str], now_ms: int) -> list[dict]:
    rows = []
    for symbol in symbols:
        open_price = round(random.uniform(50.0, 500.0), 2)
        close_price = round(open_price + random.uniform(-5.0, 5.0), 2)
        high_price = round(max(open_price, close_price) + random.uniform(0.1, 2.5), 2)
        low_price = round(min(open_price, close_price) - random.uniform(0.1, 2.5), 2)
        change = round(close_price - open_price, 6)
        change_pct = round((change / open_price * 100.0), 6) if open_price else 0.0
        rows.append({
            "symbol": symbol,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "price": close_price,
            "volume": random.randint(10_000, 1_000_000),
            "timestamp": now_ms,
            "change": change,
            "change_percent": change_pct,
            "currency": "USD",
            "source": "mock",
        })
    return rows


def fetch_batch(symbols: list[str]) -> list[dict]:
    """Download một batch symbols bằng yf.download() (parallel threads)."""
    rows = []
    try:
        history_all = yf.download(
            tickers=" ".join(symbols),
            period="1d",
            interval="1m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        for symbol in symbols:
            try:
                history = history_all if len(symbols) == 1 else (
                    history_all[symbol]
                    if symbol in history_all.columns.get_level_values(0)
                    else None
                )

                if history is None or history.empty:
                    logging.debug("No data for %s", symbol)
                    continue

                last = history.iloc[-1]
                prev_close = float(history.iloc[-2]["Close"]) if len(history) > 1 else float(last["Close"])
                close_price = float(last["Close"])
                change = close_price - prev_close
                change_pct = (change / prev_close * 100.0) if prev_close else 0.0
                ts_ms = int(history.index[-1].timestamp() * 1000)

                rows.append({
                    "symbol": symbol,
                    "open": float(last["Open"]),
                    "high": float(last["High"]),
                    "low": float(last["Low"]),
                    "close": close_price,
                    "price": close_price,
                    "volume": int(last["Volume"]),
                    "timestamp": ts_ms,
                    "change": round(change, 6),
                    "change_percent": round(change_pct, 6),
                    "currency": "USD",
                    "source": "yfinance",
                })
            except Exception as exc:  # pylint: disable=broad-except
                logging.error("Parse error for %s: %s", symbol, exc)

    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Batch download error (%s...): %s", symbols[:3], exc)

    return rows


def fetch_latest_quotes() -> list[dict]:
    """Fetch toàn bộ SYMBOLS theo batch, trả về list dict sẵn sàng gửi Kafka."""
    rows: list[dict] = []
    batches = [SYMBOLS[i:i + BATCH_SIZE] for i in range(0, len(SYMBOLS), BATCH_SIZE)]

    logging.info(
        "Fetching %d symbols in %d batches (batch_size=%d)",
        len(SYMBOLS), len(batches), BATCH_SIZE,
    )

    for idx, batch in enumerate(batches, start=1):
        batch_rows = fetch_batch(batch)
        rows.extend(batch_rows)
        logging.info("Batch %d/%d → %d rows", idx, len(batches), len(batch_rows))

    if not rows and ENABLE_MOCK_FALLBACK:
        logging.warning("Yahoo Finance returned no data — using mock fallback for all %d symbols", len(SYMBOLS))
        rows = _make_mock_rows(SYMBOLS, int(time.time() * 1000))

    return rows


# ── Main loop ─────────────────────────────────────────────────────────────────

async def fetch_and_stream() -> None:
    if producer is None:
        raise RuntimeError("Kafka producer is not initialized. Call init_kafka() first.")

    active_producer = producer
    logging.info(
        "Start streaming S&P 500 (%d symbols) → topic '%s' every %ds",
        len(SYMBOLS), TOPIC, FETCH_INTERVAL_SECONDS,
    )

    while True:
        cycle_start = time.time()
        rows = await asyncio.to_thread(fetch_latest_quotes)

        for data in rows:
            await active_producer.send(TOPIC, data)
            logging.debug("Sent: %s @ %.4f", data["symbol"], data["price"])

        elapsed = time.time() - cycle_start
        logging.info(
            "Cycle complete: %d messages sent in %.1fs, sleeping %ds",
            len(rows), elapsed, FETCH_INTERVAL_SECONDS,
        )
        await asyncio.sleep(FETCH_INTERVAL_SECONDS)


async def main() -> None:
    await init_kafka()
    try:
        await fetch_and_stream()
    finally:
        if producer is not None:
            await producer.stop()
            logging.info("Kafka producer stopped")


if __name__ == "__main__":
    asyncio.run(main())