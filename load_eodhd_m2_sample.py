"""Load EODHD fundamentals and earnings facts into Hood BigQuery tables.

`--scope sample` is the bounded M2 proof. `--scope active` refreshes every
active common-stock ticker; ETFs and benchmarks are intentionally excluded
because they do not have corporate earnings/fundamentals.
"""

from __future__ import annotations

import datetime as dt
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from typing import Any

from google.cloud import bigquery

from eodhd_client import EODHDClient


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
DATASET = os.getenv("BIGQUERY_DATASET", "Hood")
SAMPLE_TICKERS = ("NUE", "INGM", "XMTR", "HNGE", "HTFL")
CALENDAR_START = "2024-01-01"
CALENDAR_END = "2026-12-31"
MAX_QUARTERS = 8
FUNDAMENTALS_SOURCE = "EODHD v1.1/fundamentals (provider-reported EPS)"
EARNINGS_SOURCE = "EODHD calendar/earnings (provider-reported EPS)"


def as_date(value: str | None) -> dt.date | None:
    return dt.date.fromisoformat(value[:10]) if value else None


def as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(Decimal(str(value)))


def fundamentals_rows(ticker: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    income = payload.get("Financials", {}).get("Income_Statement", {}).get("quarterly", {})
    history = payload.get("Earnings", {}).get("History", {})
    records = income.values() if isinstance(income, dict) else income
    rows = []
    for record in sorted(records, key=lambda item: item.get("date", ""), reverse=True)[:MAX_QUARTERS]:
        period_end = as_date(record.get("date"))
        if period_end is None:
            continue
        earning = history.get(record["date"], {})
        rows.append({
            "ticker": ticker,
            "period_end": period_end,
            "filed_date": as_date(record.get("filing_date") or earning.get("reportDate")),
            "sales": as_float(record.get("totalRevenue")),
            "eps": as_float(earning.get("epsActual")),
            "source": FUNDAMENTALS_SOURCE,
        })
    return rows


def calendar_rows(ticker: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    records = payload.get("earnings", []) if isinstance(payload, dict) else payload
    timing = {"BeforeMarket": "BMO", "AfterMarket": "AMC"}
    rows = []
    for record in records:
        report_date = as_date(record.get("report_date"))
        if report_date is None:
            continue
        rows.append({
            "ticker": ticker,
            "date": report_date,
            "timing": timing.get(record.get("before_after_market")),
            "eps_est": as_float(record.get("estimate")),
            "eps_act": as_float(record.get("actual")),
            # EODHD calendar responses do not contain revenue actual/estimate.
            "sales_est": None,
            "sales_act": None,
            "source": EARNINGS_SOURCE,
        })
    return rows


def ensure_tables(client: bigquery.Client) -> tuple[str, str]:
    fundamentals_id = f"{PROJECT_ID}.{DATASET}.fundamentals_quarterly"
    earnings_id = f"{PROJECT_ID}.{DATASET}.earnings_calendar"
    client.create_table(bigquery.Table(fundamentals_id, schema=[
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("period_end", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("filed_date", "DATE"), bigquery.SchemaField("sales", "FLOAT"),
        bigquery.SchemaField("eps", "FLOAT"), bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("retrieved_at", "TIMESTAMP", mode="REQUIRED"),
    ]), exists_ok=True)
    client.create_table(bigquery.Table(earnings_id, schema=[
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"), bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("timing", "STRING"), bigquery.SchemaField("eps_est", "FLOAT"),
        bigquery.SchemaField("eps_act", "FLOAT"), bigquery.SchemaField("sales_est", "FLOAT"),
        bigquery.SchemaField("sales_act", "FLOAT"), bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("retrieved_at", "TIMESTAMP", mode="REQUIRED"),
    ]), exists_ok=True)
    # Existing M2 sample tables may have been created by an earlier dry run.
    for table_id in (fundamentals_id, earnings_id):
        client.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS retrieved_at TIMESTAMP").result()
    return fundamentals_id, earnings_id


def replace_rows(client: bigquery.Client, table_id: str, tickers: list[str], rows: list[dict[str, Any]]) -> None:
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("tickers", "STRING", tickers)
    ])
    client.query(f"DELETE FROM `{table_id}` WHERE ticker IN UNNEST(@tickers)", job_config=config).result()
    if rows:
        json_rows = [
            {key: value.isoformat() if isinstance(value, (dt.date, dt.datetime)) else value for key, value in row.items()}
            for row in rows
        ]
        client.load_table_from_json(json_rows, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()


def active_stock_tickers(client: bigquery.Client) -> list[str]:
    query = f"""
        SELECT ticker FROM `{PROJECT_ID}.{DATASET}.universe`
        WHERE COALESCE(active_flag, TRUE) = TRUE AND COALESCE(type, 'stock') = 'stock'
        ORDER BY ticker
    """
    return [str(row.ticker).strip().upper() for row in client.query(query).result() if row.ticker]


def fetch_one(ticker: str, refresh: str) -> tuple[str, list[dict[str, Any]] | None, list[dict[str, Any]] | None, str | None, str | None]:
    """Fetch one ticker without deleting its existing facts if a call fails."""
    api = EODHDClient()
    fundamentals = calendar = None
    fundamentals_error = calendar_error = None
    if refresh in {"fundamentals", "both"}:
        try:
            fundamentals = fundamentals_rows(ticker, api.get_fundamentals(ticker))
        except Exception as exc:
            fundamentals_error = f"{type(exc).__name__}: {exc}"
    if refresh in {"earnings", "both"}:
        try:
            calendar = calendar_rows(ticker, api.get_earnings_calendar(ticker, CALENDAR_START, CALENDAR_END))
        except Exception as exc:
            calendar_error = f"{type(exc).__name__}: {exc}"
    return ticker, fundamentals, calendar, fundamentals_error, calendar_error


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=("sample", "active"), default="sample")
    parser.add_argument("--refresh", choices=("fundamentals", "earnings", "both"), default="both")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)
    tickers = list(SAMPLE_TICKERS) if args.scope == "sample" else active_stock_tickers(client)
    if not tickers:
        raise RuntimeError("No eligible tickers found for EODHD refresh.")
    fundamentals: list[dict[str, Any]] = []
    calendar: list[dict[str, Any]] = []
    fundamental_successes: list[str] = []
    calendar_successes: list[str] = []
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = [executor.submit(fetch_one, ticker, args.refresh) for ticker in tickers]
        for completed, future in enumerate(as_completed(futures), start=1):
            ticker, ticker_fundamentals, ticker_calendar, fundamentals_error, calendar_error = future.result()
            if ticker_fundamentals is not None:
                fundamentals.extend(ticker_fundamentals)
                fundamental_successes.append(ticker)
            if ticker_calendar is not None:
                calendar.extend(ticker_calendar)
                calendar_successes.append(ticker)
            if fundamentals_error:
                failures.append(f"{ticker} fundamentals: {fundamentals_error}")
            if calendar_error:
                failures.append(f"{ticker} earnings: {calendar_error}")
            if completed % 10 == 0 or completed == len(tickers):
                print(f"[i] Fetched {completed}/{len(tickers)} tickers", flush=True)

    retrieved_at = dt.datetime.now(dt.timezone.utc)
    for row in fundamentals + calendar:
        row["retrieved_at"] = retrieved_at

    fundamentals_id, earnings_id = ensure_tables(client)
    if fundamental_successes:
        replace_rows(client, fundamentals_id, fundamental_successes, fundamentals)
    if calendar_successes:
        replace_rows(client, earnings_id, calendar_successes, calendar)
    print(f"EODHD {args.scope}/{args.refresh}: {len(tickers)} tickers; {len(fundamentals)} fundamentals; {len(calendar)} earnings; {len(failures)} failed fact fetches")
    for failure in failures[:20]:
        print(f"[!] {failure}")


if __name__ == "__main__":
    main()
