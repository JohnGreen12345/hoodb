"""Load the bounded M2 EODHD sample into Hood BigQuery tables.

This is deliberately not a universe sweep or a scheduled job. It replaces
only the five named sample tickers in the two facts tables, retaining the
provider's reported EPS and nulling fields that EODHD does not supply.
"""

from __future__ import annotations

import datetime as dt
import os
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


def replace_sample_rows(client: bigquery.Client, table_id: str, rows: list[dict[str, Any]]) -> None:
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("tickers", "STRING", list(SAMPLE_TICKERS))
    ])
    client.query(f"DELETE FROM `{table_id}` WHERE ticker IN UNNEST(@tickers)", job_config=config).result()
    if rows:
        json_rows = [
            {key: value.isoformat() if isinstance(value, (dt.date, dt.datetime)) else value for key, value in row.items()}
            for row in rows
        ]
        client.load_table_from_json(json_rows, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()


def main() -> None:
    api = EODHDClient()
    fundamentals: list[dict[str, Any]] = []
    calendar: list[dict[str, Any]] = []
    for ticker in SAMPLE_TICKERS:
        fundamentals.extend(fundamentals_rows(ticker, api.get_fundamentals(ticker)))
        calendar.extend(calendar_rows(ticker, api.get_earnings_calendar(ticker, CALENDAR_START, CALENDAR_END)))

    retrieved_at = dt.datetime.now(dt.timezone.utc)
    for row in fundamentals + calendar:
        row["retrieved_at"] = retrieved_at

    client = bigquery.Client(project=PROJECT_ID)
    fundamentals_id, earnings_id = ensure_tables(client)
    replace_sample_rows(client, fundamentals_id, fundamentals)
    replace_sample_rows(client, earnings_id, calendar)
    print(f"Loaded M2 sample: {len(fundamentals)} fundamentals and {len(calendar)} earnings rows for {len(SAMPLE_TICKERS)} tickers")


if __name__ == "__main__":
    main()
