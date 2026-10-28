r"""
BigQuery status check — run this FIRST to see what state the cloud DB is in.

Usage (PowerShell):
    $env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\your\service-account-key.json"
    python check_bq_status.py

Reports:
- Which tables exist in pricesz.Hood
- Row counts and date coverage of Daily_prices
- Universe count vs local universe.csv
- How stale the data is (days behind today)
"""

import datetime as dt
import os

from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
DATASET = os.getenv("BIGQUERY_DATASET", "Hood")


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    print(f"=== BigQuery status: {PROJECT_ID}.{DATASET} ===\n")

    # 1. What tables exist?
    tables = list(client.list_tables(f"{PROJECT_ID}.{DATASET}"))
    if not tables:
        print("[!] Dataset exists but contains NO tables.")
        return
    print("Tables found:")
    for t in tables:
        meta = client.get_table(t.reference)
        print(f"  - {t.table_id:<25} rows={meta.num_rows:>12,}  "
              f"size={meta.num_bytes / 1e6:>8.1f} MB  "
              f"modified={meta.modified:%Y-%m-%d}")
    print()

    table_ids = {t.table_id for t in tables}

    # 2. Daily_prices coverage
    if "Daily_prices" in table_ids:
        q = f"""
            SELECT
                MIN(date) AS first_date,
                MAX(date) AS last_date,
                COUNT(*) AS n_rows,
                COUNT(DISTINCT ticker) AS n_tickers
            FROM `{PROJECT_ID}.{DATASET}.Daily_prices`
        """
        row = client.query(q).result().to_dataframe().iloc[0]
        last = row["last_date"]
        staleness = (dt.date.today() - last).days if last is not None else None
        print("Daily_prices coverage:")
        print(f"  first date : {row['first_date']}")
        print(f"  last date  : {last}")
        print(f"  rows       : {row['n_rows']:,}")
        print(f"  tickers    : {row['n_tickers']}")
        print(f"  staleness  : {staleness} days behind today")
        print()

        # Per-ticker date coverage — find laggards
        q2 = f"""
            SELECT ticker, MAX(date) AS last_date, COUNT(*) AS n
            FROM `{PROJECT_ID}.{DATASET}.Daily_prices`
            GROUP BY ticker
            ORDER BY last_date ASC
            LIMIT 10
        """
        lag = client.query(q2).result().to_dataframe()
        print("10 most-stale tickers (possible delistings / symbol changes):")
        for _, r in lag.iterrows():
            print(f"  {r['ticker']:<8} last={r['last_date']}  rows={r['n']}")
        print()
    else:
        print("[!] Daily_prices table NOT found.\n")

    # 3. Universe
    if "universe" in table_ids:
        q = f"""
            SELECT
                COUNT(*) AS total,
                COUNTIF(COALESCE(active_flag, TRUE)) AS active
            FROM `{PROJECT_ID}.{DATASET}.universe`
        """
        row = client.query(q).result().to_dataframe().iloc[0]
        print(f"Universe: {row['total']} tickers total, {row['active']} active")
    else:
        print("[!] universe table NOT found.")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
