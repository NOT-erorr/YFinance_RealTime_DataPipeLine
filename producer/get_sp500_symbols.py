"""
get_sp500_symbols.py
--------------------
Lấy danh sách 503 symbols S&P 500 từ Wikipedia và lưu vào file sp500_symbols.json.
Chạy script này một lần trên máy local trước khi deploy pipeline.

Usage:
    python get_sp500_symbols.py
    python get_sp500_symbols.py --output symbols.json
"""

import argparse
import json
import logging
import sys

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
DEFAULT_OUTPUT = "sp500_symbols.json"


def fetch_sp500_symbols(url: str = WIKIPEDIA_URL) -> list[str]:
    """Scrape S&P 500 symbols from Wikipedia and return as a list."""
    logging.info("Fetching S&P 500 component table from Wikipedia...")
    tables = pd.read_html(url)
    df = tables[0]

    if "Symbol" not in df.columns:
        raise ValueError(f"Column 'Symbol' not found. Available columns: {df.columns.tolist()}")

    symbols = df["Symbol"].tolist()

    # yfinance dùng dấu '-' thay vì '.' (vd: BRK.B -> BRK-B)
    symbols = [s.replace(".", "-") for s in symbols]

    # Dedup giữ thứ tự
    seen: set[str] = set()
    symbols = [s for s in symbols if not (s in seen or seen.add(s))]  # type: ignore[func-returns-value]

    logging.info("Fetched %d symbols", len(symbols))
    return symbols


def save_symbols(symbols: list[str], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(symbols, f, indent=2)
    logging.info("Saved to %s", output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch S&P 500 symbols from Wikipedia")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output JSON file path")
    args = parser.parse_args()

    try:
        symbols = fetch_sp500_symbols()
        save_symbols(symbols, args.output)
        print(f"Done! {len(symbols)} symbols saved to '{args.output}'")
        print(symbols)
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Failed to fetch symbols: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
