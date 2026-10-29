r"""
Push the rebuilt universe.csv (DATA_MODEL.md D1 shape: ticker, name, type,
source, sector, industry, active_flag, added_at, classified_at,
inactive_reason) to
BigQuery, replacing the `universe` table's schema and contents.

Run build_universe.py then classify_universe.py first.

Usage (PowerShell, from the bigQuery folder):
    $env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\Mindrix\.gcloud\pricesz-key.json"
    python rebuild_universe_table.py
"""

import os

import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
DATASET = os.getenv("BIGQUERY_DATASET", "Hood")
UNIVERSE_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universe.csv")

SCHEMA = [
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


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    df = pd.read_csv(UNIVERSE_CSV, keep_default_na=False)
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["name"] = df["name"].fillna(df["ticker"]).astype(str)
    df["type"] = df["type"].fillna("stock").astype(str)
    df["source"] = df["source"].astype(str)
    df["sector"] = df["sector"].replace("", None)
    df["industry"] = df["industry"].replace("", None)
    df["active_flag"] = df["active_flag"].astype(str).str.lower().map({"true": True, "false": False})
    if df["active_flag"].isna().any():
        raise ValueError("active_flag must contain only True/False values")
    df["added_at"] = pd.to_datetime(df["added_at"]).dt.date
    df["classified_at"] = df["classified_at"].replace("", None)
    df["classified_at"] = pd.to_datetime(df["classified_at"], errors="coerce").dt.date
    df["inactive_reason"] = df["inactive_reason"].replace("", None)

    target = f"{PROJECT_ID}.{DATASET}.universe"
    job = client.load_table_from_dataframe(
        df,
        target,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    print(f"[+] universe table replaced: {len(df)} rows loaded into {target}")

    counts = df["source"].value_counts().to_dict()
    print(f"[i] by source: {counts}")


if __name__ == "__main__":
    main()
