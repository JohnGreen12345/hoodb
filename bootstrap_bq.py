r"""
Bootstrap the pricesz.Hood dataset from scratch.

Recreates what the BigQuery sandbox expired:
1. `universe` table  <- loaded from local universe.csv
2. `Daily_prices` table <- created empty with correct schema;
   ingest_update.py then backfills it.

Usage (PowerShell, from the bigQuery folder):
    $env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\Mindrix\.gcloud\pricesz-key.json"
    python bootstrap_bq.py
    python ingest_update.py --full-refresh-years 3
"""

import os

import pandas as pd
from google.api_core.exceptions import Forbidden
from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
DATASET = os.getenv("BIGQUERY_DATASET", "Hood")
UNIVERSE_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universe.csv")


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    ds_ref = f"{PROJECT_ID}.{DATASET}"

    # --- 0. Warn if dataset has a default table/partition expiration (sandbox
    #        symptom -- this is exactly what silently emptied Daily_prices:
    #        default_partition_expiration_ms is keyed to each partition's own
    #        date, not load time, so a partition dated more than N days ago
    #        expires the instant it's loaded) ---
    ds = client.get_dataset(ds_ref)
    expiring_fields = [f for f in ("default_table_expiration_ms", "default_partition_expiration_ms") if getattr(ds, f)]
    if expiring_fields:
        for field in expiring_fields:
            days = getattr(ds, field) / 86_400_000
            print(f"[!] WARNING: dataset {field} = {days:.0f} days.")
            setattr(ds, field, None)
        print("    Tables/partitions WILL self-delete again. Removing the expiration now...")
        try:
            client.update_dataset(ds, expiring_fields)
            print("    Done - but this only sticks if billing is enabled on the project.")
        except Forbidden as exc:
            print("    Could not remove expiration because billing is not enabled.")
            print("    Continuing with BigQuery sandbox limits; tables may expire.")
            print(f"    BigQuery said: {exc.message}")

    # --- 1. universe table from local CSV ---
    df = pd.read_csv(UNIVERSE_CSV, keep_default_na=False)
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["name"] = df["name"].fillna(df["ticker"]).astype(str)
    df["type"] = df["type"].fillna("stock").astype(str)
    df["active_flag"] = df["active_flag"].fillna(True).astype(str).str.lower().map({"true": True, "false": False})
    if df["active_flag"].isna().any():
        raise ValueError("active_flag must contain only True/False values")
    df["sector"] = df["sector"].replace("", None)
    df["industry"] = df["industry"].replace("", None)
    df["added_at"] = pd.to_datetime(df["added_at"]).dt.date
    df["classified_at"] = pd.to_datetime(df["classified_at"], errors="coerce").dt.date
    df["inactive_reason"] = df["inactive_reason"].replace("", None)

    universe_schema = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("industry", "STRING"),
        bigquery.SchemaField("active_flag", "BOOLEAN"),
        bigquery.SchemaField("added_at", "DATE"),
        bigquery.SchemaField("classified_at", "DATE"),
        bigquery.SchemaField("inactive_reason", "STRING"),
    ]
    job = client.load_table_from_dataframe(
        df,
        f"{ds_ref}.universe",
        job_config=bigquery.LoadJobConfig(
            schema=universe_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    print(f"[+] universe table created: {len(df)} tickers loaded from universe.csv")

    # --- 2. Daily_prices, empty, partitioned by date ---
    price_schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("adj_close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
    ]
    table = bigquery.Table(f"{ds_ref}.Daily_prices", schema=price_schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="date"
    )
    client.create_table(table, exists_ok=True)
    print("[+] Daily_prices table created (empty, month-partitioned)")

    print("\nNext step:  python ingest_update.py --full-refresh-years 3")
    print("(Empty table -> the ingest script backfills 3 years automatically.)")


if __name__ == "__main__":
    main()
