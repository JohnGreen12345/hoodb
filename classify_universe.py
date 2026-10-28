r"""
Fill universe.csv's sector/industry columns from Yahoo Finance, for every
row with type == 'stock' (DATA_MODEL.md sec1: "Yahoo sector + industry per
stock" -- ETFs/benchmark rows are skipped, they aren't "stock" classified
the same way and most of them already carry their own sector meaning via
their name, e.g. the XL* sector ETFs).

"Current classification, not point-in-time" (DATA_MODEL.md) -- meant to be
re-run monthly; classified_at records when each row was last pulled so a
future monthly job can refresh only stale rows.

Usage:
    python classify_universe.py [--workers 10] [--only-missing]
Rewrites universe.csv in place.
"""

import argparse
import datetime as dt
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
UNIVERSE_CSV = os.path.join(HERE, "universe.csv")
YAHOO_SYMBOL_MAP = {"VIX": "^VIX"}


def to_yahoo_symbol(ticker: str) -> str:
    if ticker in YAHOO_SYMBOL_MAP:
        return YAHOO_SYMBOL_MAP[ticker]
    return ticker.replace(".", "-")


def fetch_one(ticker: str, retries: int = 5, request_delay: float = 1.5) -> tuple[str, str | None, str | None]:
    """Yahoo rate-limits hard under concurrency (confirmed live: a 10-worker
    run returned YFRateLimitError for every single ticker). Serial calls
    only, a fixed delay before each request, and a long exponential
    backoff (not the short one that caused the all-failed run) specifically
    on rate-limit errors."""
    yahoo_ticker = to_yahoo_symbol(ticker)
    for attempt in range(retries + 1):
        time.sleep(request_delay)
        try:
            info = yf.Ticker(yahoo_ticker).get_info()
            sector = info.get("sector") or None
            industry = info.get("industry") or None
            return ticker, sector, industry
        except Exception as exc:
            is_rate_limit = "Rate limited" in str(exc) or "Too Many Requests" in str(exc)
            if attempt == retries:
                print(f"[!] {ticker}: giving up after {retries + 1} attempts ({exc})")
                return ticker, None, None
            backoff = 30 * (attempt + 1) if is_rate_limit else 2 * (attempt + 1)
            print(f"[i] {ticker}: {exc} -- retrying in {backoff}s")
            time.sleep(backoff)
    return ticker, None, None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--only-missing", action="store_true", help="skip rows that already have a sector")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    today = dt.date.today().isoformat()

    df = pd.read_csv(UNIVERSE_CSV, keep_default_na=False)
    df["ticker"] = df["ticker"].astype(str).str.strip()

    is_stock = df["type"] == "stock"
    if args.only_missing:
        todo_mask = is_stock & (df["sector"].astype(str).str.strip() == "")
    else:
        todo_mask = is_stock
    todo_tickers = df.loc[todo_mask, "ticker"].tolist()

    print(f"[i] classifying {len(todo_tickers)} stock ticker(s) with {args.workers} worker(s)")

    results: dict[str, tuple[str | None, str | None]] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_one, t): t for t in todo_tickers}
        for fut in as_completed(futures):
            ticker, sector, industry = fut.result()
            results[ticker] = (sector, industry)
            done += 1
            if done % 100 == 0:
                print(f"[i] {done}/{len(todo_tickers)}")

    filled = 0
    for ticker, (sector, industry) in results.items():
        idx = df.index[df["ticker"] == ticker]
        if len(idx) == 0:
            continue
        i = idx[0]
        if sector:
            df.at[i, "sector"] = sector
            filled += 1
        if industry:
            df.at[i, "industry"] = industry
        df.at[i, "classified_at"] = today

    df.to_csv(UNIVERSE_CSV, index=False)

    n_stock = int(is_stock.sum())
    n_classified = int((df.loc[is_stock, "sector"].astype(str).str.strip() != "").sum())
    pct = 100.0 * n_classified / n_stock if n_stock else 0.0
    print(f"[+] filled sector for {filled} ticker(s) this run")
    print(f"[+] classification coverage: {n_classified}/{n_stock} stocks ({pct:.1f}%)")


if __name__ == "__main__":
    main()
