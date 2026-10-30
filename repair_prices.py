"""Safely replace explicitly named Hood price bars and retain provenance.

The script is dry-run by default. It only deletes rows after Yahoo has
returned every requested date and each replacement passes basic OHLC checks.

Example:
    python repair_prices.py --ticker NUE --date 2026-10-28 --date 2026-10-29 \
        --reason "Correct incomplete Yahoo rows" --apply
"""

import argparse
import datetime as dt
import json
import os
import uuid

import pandas as pd
from google.cloud import bigquery

from ingest_update import fetch_yahoo_prices


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
DATASET = os.getenv("BIGQUERY_DATASET", "Hood")
PRICE_TABLE = "Daily_prices"
REPAIR_TABLE = "price_repairs"
SOURCE = "Yahoo Finance via yfinance date-scoped re-fetch"


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def validate_replacement(rows: pd.DataFrame, ticker: str, requested_dates: set[dt.date]) -> None:
    """Reject incomplete, duplicate, or impossible OHLC replacement bars."""
    if set(rows["ticker"]) != {ticker}:
        raise ValueError("Yahoo response has an unexpected ticker.")
    if set(rows["date"]) != requested_dates or rows["date"].duplicated().any():
        raise ValueError("Yahoo response does not contain exactly the requested dates once each.")
    invalid = (rows["high"] < rows[["open", "close", "low"]].max(axis=1)) | (
        rows["low"] > rows[["open", "close", "high"]].min(axis=1)
    )
    if invalid.any():
        bad_dates = ", ".join(rows.loc[invalid, "date"].astype(str))
        raise ValueError(f"Yahoo response has invalid OHLC values on: {bad_dates}")


def ensure_repair_table(client: bigquery.Client) -> str:
    table_id = f"{PROJECT_ID}.{DATASET}.{REPAIR_TABLE}"
    schema = [
        bigquery.SchemaField("repair_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("reason", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("old_bar_json", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("new_bar_json", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("repaired_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
    return table_id


def existing_rows(client: bigquery.Client, ticker: str, dates: list[dt.date]) -> dict[dt.date, dict]:
    table_id = f"{PROJECT_ID}.{DATASET}.{PRICE_TABLE}"
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
        bigquery.ArrayQueryParameter("dates", "DATE", dates),
    ])
    rows = client.query(
        f"SELECT date, open, high, low, close, adj_close, volume FROM `{table_id}` "
        "WHERE ticker = @ticker AND date IN UNNEST(@dates)",
        job_config=job_config,
    ).result()
    result: dict[dt.date, dict] = {}
    for row in rows:
        if row.date in result:
            raise ValueError(f"Duplicate existing bar for {ticker} on {row.date}.")
        result[row.date] = {key: row[key] for key in row.keys()}
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--date", action="append", type=parse_date, required=True,
                        help="One exact session date to replace; repeat for each date.")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--apply", action="store_true", help="Perform the replacement; otherwise only validate.")
    args = parser.parse_args()

    dates = sorted(set(args.date))
    fetched = fetch_yahoo_prices([args.ticker], min(dates), max(dates))
    fetched = fetched[fetched["date"].isin(dates)].copy()
    validate_replacement(fetched, args.ticker, set(dates))

    client = bigquery.Client(project=PROJECT_ID)
    old = existing_rows(client, args.ticker, dates)
    if set(old) != set(dates):
        raise ValueError("Hood does not contain exactly one existing bar for every requested date.")
    print(f"Validated {len(fetched)} {args.ticker} Yahoo replacement bar(s); apply={args.apply}")
    if not args.apply:
        return

    table_id = f"{PROJECT_ID}.{DATASET}.{PRICE_TABLE}"
    repair_id = str(uuid.uuid4())
    repaired_at = dt.datetime.now(dt.timezone.utc)
    new_by_date = {row["date"]: row for row in fetched.to_dict(orient="records")}
    log_rows = []
    for date in dates:
        new = new_by_date[date]
        log_rows.append({
            "repair_id": repair_id,
            "ticker": args.ticker,
            "date": date.isoformat(),
            "source": SOURCE,
            "reason": args.reason,
            "old_bar_json": json.dumps(old[date], default=str, sort_keys=True),
            "new_bar_json": json.dumps(new, default=str, sort_keys=True),
            "repaired_at": repaired_at.isoformat(),
        })
    errors = client.insert_rows_json(ensure_repair_table(client), log_rows)
    if errors:
        raise RuntimeError(f"Could not record repair provenance: {errors}")

    delete_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("ticker", "STRING", args.ticker),
        bigquery.ArrayQueryParameter("dates", "DATE", dates),
    ])
    client.query(
        f"DELETE FROM `{table_id}` WHERE ticker = @ticker AND date IN UNNEST(@dates)",
        job_config=delete_config,
    ).result()
    load_job = client.load_table_from_dataframe(
        fetched, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    load_job.result()
    print(f"Repaired {len(fetched)} bar(s); provenance repair_id={repair_id}")


if __name__ == "__main__":
    main()
